package jsonresponse

import "github.com/gofiber/fiber/v2"

func OK(ctx *fiber.Ctx, message string, data any) error {
	return ctx.Status(200).JSON(&fiber.Map{
		"message": message,
		"data":    data,
	})
}

func NotFound(ctx *fiber.Ctx, message string) error {
	return ctx.Status(404).JSON(&fiber.Map{
		"message": message,
	})
}

func BadRequest(ctx *fiber.Ctx, message string) error {
	return ctx.Status(400).JSON(&fiber.Map{
		"message": message,
	})
}

func ServerError(ctx *fiber.Ctx, message string) error {
	return ctx.Status(500).JSON(&fiber.Map{
		"message": message,
	})
}
