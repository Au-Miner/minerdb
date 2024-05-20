package min

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newTestRouter() *router {
	r := newRouter()
	r.addRoute("GET", "/", nil)
	r.addRoute("GET", "/hello/:name", nil)
	r.addRoute("GET", "/hello/b/c", nil)
	r.addRoute("GET", "/hi/:name", nil)
	r.addRoute("GET", "/assets/*filepath", nil)
	return r
}

func TestParsePattern(t *testing.T) {
	r := newRouter()
	ok := reflect.DeepEqual(r.parsePattern("/p/:name"), []string{"p", ":name"})
	ok = ok && reflect.DeepEqual(r.parsePattern("/p/*"), []string{"p", "*"})
	ok = ok && reflect.DeepEqual(r.parsePattern("/p/*name/*"), []string{"p", "*name"})

	assert.True(t, ok, "test parsePattern failed")
}

func TestGetRoute(t *testing.T) {
	r := newTestRouter()
	n, ps := r.getRoute("GET", "/hello/min")

	assert.NotNil(t, n, "nil shouldn't be returned")
	assert.Equal(t, "/hello/:name", n.pattern, "should match /hello/:name")
	assert.Equal(t, "min", ps["name"], "name should be equal to 'min'")

	n, _ = r.getRoute("GET", "/hello/min/aaa")
	assert.Nil(t, n, "nil shouldn't be returned")
}

func TestGetRoute2(t *testing.T) {
	r := newTestRouter()
	n1, ps1 := r.getRoute("GET", "/assets/file1.txt")

	assert.Equal(t, "/assets/*filepath", n1.pattern)
	assert.Equal(t, "file1.txt", ps1["filepath"])

	n2, ps2 := r.getRoute("GET", "/assets/css/test.css")
	assert.Equal(t, "/assets/*filepath", n2.pattern)
	assert.Equal(t, "css/test.css", ps2["filepath"])
}
