package jin

import (
	"fmt"
	"log"
	"net/http"
	"strings"
)

type router struct {
	roots map[string]*node
}

func newRouter() *router {
	return &router{
		roots: make(map[string]*node),
	}
}

func (r *router) addRoute(method string, pattern string, handlers []HandlerFunc) {
	parts := r.parsePattern(pattern)
	log.Printf("parsePattern %4s - %s", pattern, parts)
	_, ok := r.roots[method]
	if !ok {
		r.roots[method] = &node{}
	}
	r.roots[method].insert(handlers, pattern, parts, 0)
}

func (r *router) handle(c *Context) {
	n, params := r.getRoute(c.Method, c.Path)
	if n == nil {
		c.String(http.StatusNotFound, "JIN 404 NOT FOUND: %s\n", c.Path)
		return
	}
	c.Params = params
	c.handlers = n.handlers
	fmt.Println("DEBUG - ", c.Req.URL, " -> ", n.pattern)
	c.Next()
}

func (r *router) getRoute(method string, path string) (*node, map[string]string) {
	searchParts := r.parsePattern(path)

	params := make(map[string]string)

	root, ok := r.roots[method]
	if !ok {
		return nil, nil
	}

	leaf_node := root.search(searchParts, 0)
	if leaf_node == nil {
		return nil, nil
	}

	parts := r.parsePattern(leaf_node.pattern)
	for index, part := range parts {
		if part[0] == ':' {
			params[part[1:]] = searchParts[index]
		} else if part[0] == '*' && len(part) > 1 {
			params[part[1:]] = strings.Join(searchParts[index:], "/")
		}
	}

	return leaf_node, params
}

func (r *router) parsePattern(pattern string) []string {
	splits := strings.Split(pattern, "/")
	parts := make([]string, 0)
	for _, item := range splits {
		if item != "" {
			parts = append(parts, item)
		}
		if len(item) > 0 && item[0] == '*' {
			break
		}
	}
	return parts
}
