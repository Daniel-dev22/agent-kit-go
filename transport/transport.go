// Package transport provides the outbound HTTP building blocks an agent needs
// to call back into its controller: a constant-bearer round-tripper and a
// pooled *http.Transport that (optionally) rewrites the dial target to a local
// reverse proxy while preserving the URL authority as the Host header + TLS
// SNI. That rewrite is what lets an agent address the controller by its public
// name while the bytes actually go to a sidecar proxy on the same host, which
// is where mutual TLS is attached.
//
// It exists because two agents had built the same client independently and
// differed only in a few pool knobs. Those differences are quoted in
// NewRouterTransport so adopting this package is a deliberate
// behavioural-parity decision rather than an accident.
package transport

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Canonical pool/timeout settings. The two pre-existing implementations agreed
// on the shared shape (MaxIdleConns 20, MaxIdleConnsPerHost 10, Dialer.Timeout
// 5s, Client 30s) and differed only on the rows marked (*), reconciled here to
// the longer-lived, HTTP/2-enabled values:
//
//	                      impl A         impl B             canonical (here)
//	Dialer.Timeout        5s             5s                 5s
//	Dialer.KeepAlive (*)  30s            15s                30s
//	MaxIdleConns          20             20                 20
//	MaxIdleConnsPerHost   10             10                 10
//	IdleConnTimeout  (*)  90s            0 (unbounded)      90s
//	ForceAttemptHTTP2(*)  true           unset (false)      true
//	http.Client.Timeout   30s            30s                30s
const (
	dialTimeout         = 5 * time.Second
	dialKeepAlive       = 30 * time.Second
	maxIdleConns        = 20
	maxIdleConnsPerHost = 10
	idleConnTimeout     = 90 * time.Second
	clientTimeout       = 30 * time.Second
)

// bearerRoundTripper attaches a constant per-host bearer token to every
// outbound request that does not already carry an Authorization header,
// delegating to an underlying transport unchanged (pooling/dialing preserved).
type bearerRoundTripper struct {
	base  http.RoundTripper
	token string
}

// NewBearerRoundTripper returns a RoundTripper that sets
// "Authorization: Bearer <token>" on every request whose Authorization header
// is empty, then delegates to base. A caller-set Authorization header is never
// overwritten (so per-request overrides still work). If base is nil,
// http.DefaultTransport is used. A redirect hop that leaves the original request's
// origin (host:port) or downgrades https to http is refused with
// ErrCrossOriginRedirect: neither the token nor the body follows it.
func NewBearerRoundTripper(base http.RoundTripper, token string) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &bearerRoundTripper{base: base, token: token}
}

func (b *bearerRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	if err := checkRedirectChain(r); err != nil {
		// RoundTrip must always close the request body, including on error.
		if r.Body != nil {
			_ = r.Body.Close()
		}
		return nil, err
	}
	if b.token != "" && r.Header.Get("Authorization") == "" {
		// Clone before mutating per the net/http RoundTripper contract.
		r2 := r.Clone(r.Context())
		r2.Header.Set("Authorization", "Bearer "+b.token)
		return b.base.RoundTrip(r2)
	}
	return b.base.RoundTrip(r)
}

// ErrCrossOriginRedirect is returned (wrapped in *url.Error by http.Client)
// when a redirect hop would leave the origin of the ORIGINAL request or
// downgrade https to http. Agents only ever call their controller; a redirect
// elsewhere is a misconfiguration or a compromise, and following it would hand
// the credential and (for 307/308) the replayed request body to that host.
var ErrCrossOriginRedirect = errors.New("transport: refusing cross-origin or scheme-downgrading redirect")

// checkRedirectChain refuses a redirect hop that left the root request's
// origin. net/http sets r.Response on every redirect hop, and resp.Request is
// the PREVIOUS hop, so the chain is walked to the root and every hop compared
// against it (never against its predecessor: A -> B -> B must not look
// same-origin). Sticky by construction, like net/http's own header stripping.
func checkRedirectChain(r *http.Request) error {
	if r.Response == nil {
		return nil
	}
	root := r
	for root.Response != nil && root.Response.Request != nil {
		root = root.Response.Request
	}
	if root == r || root.URL == nil {
		return nil
	}
	for h := r; h != nil && h != root; {
		if h.URL == nil || leavesOrigin(root.URL, h.URL) {
			return ErrCrossOriginRedirect
		}
		if h.Response == nil {
			break
		}
		h = h.Response.Request
	}
	return nil
}

// leavesOrigin reports whether next is a different origin from root or drops
// below an https root. The single rule shared by the RoundTripper check and
// the client CheckRedirect.
func leavesOrigin(root, next *url.URL) bool {
	return originOf(next) != originOf(root) ||
		(strings.EqualFold(root.Scheme, "https") && !strings.EqualFold(next.Scheme, "https"))
}

// maxRedirects mirrors net/http's default limit, which a custom CheckRedirect
// replaces.
const maxRedirects = 10

// refuseCrossOriginRedirect is the http.Client.CheckRedirect for every kit
// client, token or not: a tokenless client would otherwise follow a redirect to
// another origin and accept the foreign response as the controller's answer
// (e.g. secrets vend). via[0] is the original request.
func refuseCrossOriginRedirect(req *http.Request, via []*http.Request) error {
	if len(via) == 0 {
		return nil
	}
	if len(via) >= maxRedirects {
		return errors.New("stopped after 10 redirects")
	}
	if via[0].URL == nil || req.URL == nil || leavesOrigin(via[0].URL, req.URL) {
		return ErrCrossOriginRedirect
	}
	return nil
}

// originOf canonicalises scheme://host:port (lower-case, no trailing dot,
// default port made explicit) so equivalent spellings of one origin compare
// equal. Anything else compares unequal, which fails closed.
func originOf(u *url.URL) string {
	scheme := strings.ToLower(u.Scheme)
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	port := u.Port()
	if port == "" {
		switch scheme {
		case "https":
			port = "443"
		case "http":
			port = "80"
		}
	}
	return scheme + "://" + net.JoinHostPort(host, port)
}

// NewRouterTransport builds a pooled *http.Transport with the canonical pool
// settings above. Two dial modes:
//
//   - Direct mode (traefikDockerDNS == ""): the URL host is dialed normally.
//     Used where the agent can itself reach the destination (tests, k3s nodes
//     dialing cluster DNS/CNI).
//
//   - Traefik-rewrite mode (traefikDockerDNS != ""): DialContext ignores the
//     URL's host:port and dials <traefikDockerDNS>:<traefikDialPort> instead —
//     the local docker Traefik container, resolved via the docker network's
//     embedded DNS. The HTTP Host header and TLS SNI stay as the URL authority
//     so Traefik matches its inbound rule and forwards into the cluster over
//     its site-specific mTLS ServersTransport. Traefik is re-dialed on every
//     connect so a Traefik restart is picked up without an agent restart.
func NewRouterTransport(traefikDockerDNS, traefikDialPort string) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: dialKeepAlive,
	}
	t := &http.Transport{
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        maxIdleConns,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		IdleConnTimeout:     idleConnTimeout,
	}
	if traefikDockerDNS != "" {
		target := net.JoinHostPort(traefikDockerDNS, traefikDialPort)
		t.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, target)
		}
	} else {
		t.DialContext = dialer.DialContext
	}
	return t
}

// RouterClientConfig configures NewRouterClient. It is deliberately generic —
// no gdrive/duplicacy-specific fields — so any agent can compose it.
type RouterClientConfig struct {
	// TraefikDockerDNS, when non-empty, rewrites every dial target to
	// <TraefikDockerDNS>:<TraefikDialPort> (see NewRouterTransport).
	TraefikDockerDNS string
	// TraefikDialPort is the port dialed on TraefikDockerDNS in rewrite mode.
	TraefikDialPort string
	// BearerToken, when non-empty, is attached as "Authorization: Bearer …"
	// on every request that does not already carry an Authorization header.
	BearerToken string
}

// NewRouterClient composes NewRouterTransport with the canonical client
// timeout and, when cfg.BearerToken is set, an outer NewBearerRoundTripper.
// Whatever the token, the client refuses any redirect that leaves the original
// request's origin (ErrCrossOriginRedirect). The returned client is safe for
// concurrent use and should be process-global (its transport owns the
// connection pool).
func NewRouterClient(cfg RouterClientConfig) *http.Client {
	var rt http.RoundTripper = NewRouterTransport(cfg.TraefikDockerDNS, cfg.TraefikDialPort)
	if cfg.BearerToken != "" {
		rt = NewBearerRoundTripper(rt, cfg.BearerToken)
	}
	return &http.Client{
		Timeout:       clientTimeout,
		Transport:     rt,
		CheckRedirect: refuseCrossOriginRedirect,
	}
}
