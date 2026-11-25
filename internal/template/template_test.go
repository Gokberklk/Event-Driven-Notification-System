package template

import "testing"

func TestVariables(t *testing.T) {
	got := Variables("Hi {{ name }}, {{orderId}} for {{name}}")
	if len(got) != 2 || got[0] != "name" || got[1] != "orderId" {
		t.Fatalf("got %v", got)
	}
}

func TestRender(t *testing.T) {
	out, err := Render("Hi {{ name }}, order {{orderId}}", map[string]string{"name": "Ali", "orderId": "7", "extra": "x"})
	if err != nil || out != "Hi Ali, order 7" {
		t.Fatalf("out=%q err=%v", out, err)
	}
	if _, err := Render("Hi {{name}} {{code}}", map[string]string{"name": "x"}); err == nil {
		t.Fatal("missing variable must fail")
	}
	out, _ = Render("{{a}}", map[string]string{"a": "{{b}}", "b": "nope"})
	if out != "{{b}}" {
		t.Fatalf("values must not be re-rendered, got %q", out)
	}
}

func TestValidate(t *testing.T) {
	ok := CreateRequest{Name: " Order-Shipped ", Channel: "SMS", Body: "Hi {{name}}"}
	ok.Normalize()
	if err := ok.Validate(); err != nil || ok.Name != "order-shipped" || ok.Channel != "sms" {
		t.Fatalf("err=%v req=%+v", err, ok)
	}
	bad := CreateRequest{Name: "has space", Channel: "fax", Body: "Hi {{name}"}
	err, isVE := bad.Validate().(*ValidationError)
	if !isVE || len(err.Fields) != 3 {
		t.Fatalf("want 3 field errors, got %v", err)
	}
}
