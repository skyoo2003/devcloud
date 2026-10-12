// SPDX-License-Identifier: Apache-2.0
package lambda

import (
	"context"
	"fmt"

	"github.com/skyoo2003/devcloud/internal/shared/workload"
)

func runtimeImage(runtime string) (string, error) {
	switch runtime {
	case "python3.12":
		return "public.ecr.aws/lambda/python:3.12", nil
	case "nodejs22.x":
		return "public.ecr.aws/lambda/nodejs:22", nil
	default:
		return "", fmt.Errorf("%w: %s", ErrUnsupportedRuntime, runtime)
	}
}
func runDocker(ctx context.Context, input []byte, args ...string) ([]byte, error) {
	return workload.DefaultContainerRunner()(ctx, input, args...)
}
