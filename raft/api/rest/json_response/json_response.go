package json_response

import (
	"minerdb/jin"
	"net/http"
)

func OK(ctx *jin.Context, message string, data any) {
	ctx.JSON(http.StatusOK, map[string]any{"message": message, "data": data})
}

func NotFound(ctx *jin.Context, message string) {
	ctx.Fail(http.StatusNotFound, message)
}

func BadRequest(ctx *jin.Context, message string) {
	ctx.Fail(http.StatusBadRequest, message)
}

func ServerError(ctx *jin.Context, message string) {
	ctx.Fail(http.StatusInternalServerError, message)
}
