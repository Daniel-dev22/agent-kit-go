package scheduler

// Param helpers: each FireFunc deals with a map[string]interface{} that
// arrived via JSON, so numeric values are float64, etc. These helpers
// extract typed values without panicking when a key is missing or has an
// unexpected type — the unhappy path returns the zero value for the type.

// ParamString returns m[k] coerced to string, or "" if missing or wrong type.
func ParamString(m map[string]interface{}, k string) string {
	if v, ok := m[k]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// ParamInt returns m[k] coerced to int. Handles both float64 (JSON's
// default for numbers) and native int. Zero on miss.
func ParamInt(m map[string]interface{}, k string) int {
	if v, ok := m[k]; ok {
		switch n := v.(type) {
		case float64:
			return int(n)
		case int:
			return n
		}
	}
	return 0
}

// ParamBool returns m[k] coerced to bool. False on miss or wrong type.
func ParamBool(m map[string]interface{}, k string) bool {
	if v, ok := m[k]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

// ParamStrings returns m[k] coerced to []string. Empty slice on miss or
// wrong type. Skips non-string elements without erroring.
func ParamStrings(m map[string]interface{}, k string) []string {
	if v, ok := m[k]; ok {
		if arr, ok := v.([]interface{}); ok {
			out := make([]string, 0, len(arr))
			for _, e := range arr {
				if s, ok := e.(string); ok {
					out = append(out, s)
				}
			}
			return out
		}
	}
	return nil
}
