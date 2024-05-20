package json_response

import (
	"minerdb/min"
	"net/http"
)

func OK(ctx *min.Context, message string, data any) {
	ctx.JSON(http.StatusOK, map[string]any{"message": message, "data": data})
}

func NotFound(ctx *min.Context, message string) {
	ctx.Fail(http.StatusNotFound, message)
}

func BadRequest(ctx *min.Context, message string) {
	ctx.Fail(http.StatusBadRequest, message)
}

func ServerError(ctx *min.Context, message string) {
	ctx.Fail(http.StatusInternalServerError, message)
}
