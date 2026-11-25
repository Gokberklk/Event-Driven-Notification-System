// Package template implements reusable message templates with
// {{variable}} substitution.
package template

import (
	"errors"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Template is a reusable message body for one channel.
type Template struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Channel   string    `json:"channel"`
	Body      string    `json:"body"`
	Variables []string  `json:"variables"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// CreateRequest is the input for creating a template.
type CreateRequest struct {
	Name    string `json:"name"`
	Channel string `json:"channel"`
	Body    string `json:"body"`
}

var (
	ErrNotFound      = errors.New("template not found")
	ErrDuplicateName = errors.New("a template with this name already exists")

	placeholderRe = regexp.MustCompile(`\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}`)
	nameRe        = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,99}$`)
)

// ValidationError reports invalid template input or missing variables.
type ValidationError struct {
	Fields map[string]string
}

func (e *ValidationError) Error() string {
	parts := make([]string, 0, len(e.Fields))
	for k, v := range e.Fields {
		parts = append(parts, k+": "+v)
	}
	sort.Strings(parts)
	return "template validation failed: " + strings.Join(parts, "; ")
}

// Normalize trims input and lower-cases name and channel.
func (r *CreateRequest) Normalize() {
	r.Name = strings.ToLower(strings.TrimSpace(r.Name))
	r.Channel = strings.ToLower(strings.TrimSpace(r.Channel))
}

// Validate checks the request. Content length is checked when a
// notification is rendered, because it depends on the variable values.
func (r *CreateRequest) Validate() error {
	f := map[string]string{}
	if !nameRe.MatchString(r.Name) {
		f["name"] = "must be 1-100 chars of lowercase letters, digits, '-' or '_'"
	}
	switch r.Channel {
	case "sms", "email", "push":
	default:
		f["channel"] = "must be one of sms, email, push"
	}
	if strings.TrimSpace(r.Body) == "" {
		f["body"] = "is required"
	} else if strings.Count(r.Body, "{{") != strings.Count(r.Body, "}}") {
		f["body"] = "has unbalanced {{ }} placeholders"
	}
	if len(f) > 0 {
		return &ValidationError{Fields: f}
	}
	return nil
}

// Variables returns the unique placeholder names in body, sorted.
func Variables(body string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, m := range placeholderRe.FindAllStringSubmatch(body, -1) {
		if !seen[m[1]] {
			seen[m[1]] = true
			out = append(out, m[1])
		}
	}
	sort.Strings(out)
	return out
}

// Render substitutes every {{name}} in body. All placeholders must have a
// value; extra variables are ignored.
func Render(body string, vars map[string]string) (string, error) {
	var missing []string
	for _, v := range Variables(body) {
		if _, ok := vars[v]; !ok {
			missing = append(missing, v)
		}
	}
	if len(missing) > 0 {
		return "", &ValidationError{Fields: map[string]string{
			"variables": "missing values for: " + strings.Join(missing, ", "),
		}}
	}
	return placeholderRe.ReplaceAllStringFunc(body, func(m string) string {
		return vars[placeholderRe.FindStringSubmatch(m)[1]]
	}), nil
}
