// SPDX-License-Identifier: Apache-2.0
package lambda

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
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
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = bytes.NewReader(input)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%w: %s", err, stderr.String())
	}
	if len(args) > 0 && args[0] == "logs" {
		// Docker forwards the container's stderr separately from stdout.
		stdout.Write(stderr.Bytes())
	}
	return stdout.Bytes(), nil
}
