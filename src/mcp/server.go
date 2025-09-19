package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/f1bonacc1/process-compose/src/app"
	"github.com/f1bonacc1/process-compose/src/config"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type MCPServer struct {
	server        *mcp.Server
	projectRunner app.IProject
}

func NewMCPServer(projectRunner app.IProject) (*MCPServer, error) {
	implementation := &mcp.Implementation{
		Name:    "process-compose",
		Version: config.Version,
	}

	options := &mcp.ServerOptions{
		HasTools:     true,
		HasResources: true,
	}

	server := mcp.NewServer(implementation, options)

	mcpServer := &MCPServer{
		server:        server,
		projectRunner: projectRunner,
	}

	if err := mcpServer.registerTools(); err != nil {
		return nil, fmt.Errorf("failed to register tools: %w", err)
	}

	if err := mcpServer.registerResources(); err != nil {
		return nil, fmt.Errorf("failed to register resources: %w", err)
	}

	return mcpServer, nil
}

func (s *MCPServer) ServeStdio(ctx context.Context) error {
	transport := &mcp.StdioTransport{}
	return s.server.Run(ctx, transport)
}

type EmptyArgs struct{}

type ProcessNameArgs struct {
	Name string `json:"name" jsonschema:"Name of the process"`
}

type ScaleArgs struct {
	Name     string `json:"name" jsonschema:"Name of the process to scale"`
	Replicas int    `json:"replicas" jsonschema:"Number of replicas to scale to"`
}

type LogArgs struct {
	Name      string `json:"name" jsonschema:"Name of the process"`
	EndOffset int    `json:"endOffset,omitempty" jsonschema:"Offset from the end of the log"`
	Limit     int    `json:"limit,omitempty" jsonschema:"Limit of lines to get (0 for all)"`
}

type ProjectStateArgs struct {
	WithMemory bool `json:"withMemory,omitempty" jsonschema:"Include memory usage information"`
}

func (s *MCPServer) registerTools() error {
	mcp.AddTool(s.server, &mcp.Tool{
		Name:        "pc.list_processes",
		Description: "List all processes and their current states",
	}, s.handleListProcesses)

	mcp.AddTool(s.server, &mcp.Tool{
		Name:        "pc.get_process",
		Description: "Get detailed information about a specific process",
	}, s.handleGetProcess)

	mcp.AddTool(s.server, &mcp.Tool{
		Name:        "pc.start_process",
		Description: "Start a process",
	}, s.handleStartProcess)

	mcp.AddTool(s.server, &mcp.Tool{
		Name:        "pc.stop_process",
		Description: "Stop a process",
	}, s.handleStopProcess)

	mcp.AddTool(s.server, &mcp.Tool{
		Name:        "pc.restart_process",
		Description: "Restart a process",
	}, s.handleRestartProcess)

	mcp.AddTool(s.server, &mcp.Tool{
		Name:        "pc.scale_process",
		Description: "Scale a process to a specific number of replicas",
	}, s.handleScaleProcess)

	mcp.AddTool(s.server, &mcp.Tool{
		Name:        "pc.get_process_logs",
		Description: "Get logs for a specific process",
	}, s.handleGetProcessLogs)

	mcp.AddTool(s.server, &mcp.Tool{
		Name:        "pc.get_project_state",
		Description: "Get the current state of the project",
	}, s.handleGetProjectState)

	return nil
}

func (s *MCPServer) registerResources() error {
	s.server.AddResource(&mcp.Resource{
		URI:         "process-compose://project/config",
		Name:        "Project Configuration",
		Description: "Current project configuration and settings",
		MIMEType:    "application/json",
	}, s.handleProjectConfigResource)

	s.server.AddResource(&mcp.Resource{
		URI:         "process-compose://processes/states",
		Name:        "Process States",
		Description: "Real-time process states and status information",
		MIMEType:    "application/json",
	}, s.handleProcessStatesResource)

	return nil
}

func (s *MCPServer) handleListProcesses(ctx context.Context, request *mcp.CallToolRequest, args EmptyArgs) (*mcp.CallToolResult, any, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	
	type processesResult struct {
		states interface{}
		err    error
	}
	
	resultChan := make(chan processesResult, 1)
	go func() {
		states, err := s.projectRunner.GetProcessesState()
		resultChan <- processesResult{states: states, err: err}
	}()
	
	select {
	case result := <-resultChan:
		if result.err != nil {
			return &mcp.CallToolResult{
				Content: []mcp.Content{
					&mcp.TextContent{Text: fmt.Sprintf("Error getting processes state: %v. This may indicate a configuration loading issue. Please check your process-compose.yaml file for missing dependencies or invalid process definitions.", result.err)},
				},
				IsError: true,
			}, nil, nil
		}

		content, err := json.MarshalIndent(result.states, "", "  ")
		if err != nil {
			return &mcp.CallToolResult{
				Content: []mcp.Content{
					&mcp.TextContent{Text: fmt.Sprintf("Error marshaling processes state: %v", err)},
				},
				IsError: true,
			}, nil, nil
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: string(content)},
			},
		}, result.states, nil
		
	case <-timeoutCtx.Done():
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: "Timeout getting processes state"},
			},
			IsError: true,
		}, nil, nil
	}
}

func (s *MCPServer) handleGetProcess(ctx context.Context, request *mcp.CallToolRequest, args ProcessNameArgs) (*mcp.CallToolResult, any, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	
	type processResult struct {
		state interface{}
		err   error
	}
	
	resultChan := make(chan processResult, 1)
	go func() {
		state, err := s.projectRunner.GetProcessState(args.Name)
		resultChan <- processResult{state: state, err: err}
	}()
	
	select {
	case result := <-resultChan:
		if result.err != nil {
			return &mcp.CallToolResult{
				Content: []mcp.Content{
					&mcp.TextContent{Text: fmt.Sprintf("Error getting process state for %s: %v. This may indicate the process is not defined in your configuration or there are dependency issues.", args.Name, result.err)},
				},
				IsError: true,
			}, nil, nil
		}
		
		content, err := json.MarshalIndent(result.state, "", "  ")
		if err != nil {
			return &mcp.CallToolResult{
				Content: []mcp.Content{
					&mcp.TextContent{Text: fmt.Sprintf("Error marshaling process state: %v", err)},
				},
				IsError: true,
			}, nil, nil
		}
		
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: string(content)},
			},
		}, result.state, nil
		
	case <-timeoutCtx.Done():
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("Timeout getting process state for %s", args.Name)},
			},
			IsError: true,
		}, nil, nil
	}
}

func (s *MCPServer) handleStartProcess(ctx context.Context, request *mcp.CallToolRequest, args ProcessNameArgs) (*mcp.CallToolResult, any, error) {
	err := s.projectRunner.StartProcess(args.Name)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("Error starting process %s: %v", args.Name, err)},
			},
			IsError: true,
		}, nil, nil
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: fmt.Sprintf("Process %s started successfully", args.Name)},
		},
	}, map[string]string{"status": "started", "process": args.Name}, nil
}

func (s *MCPServer) handleStopProcess(ctx context.Context, request *mcp.CallToolRequest, args ProcessNameArgs) (*mcp.CallToolResult, any, error) {
	err := s.projectRunner.StopProcess(args.Name)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("Error stopping process %s: %v", args.Name, err)},
			},
			IsError: true,
		}, nil, nil
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: fmt.Sprintf("Process %s stopped successfully", args.Name)},
		},
	}, map[string]string{"status": "stopped", "process": args.Name}, nil
}

func (s *MCPServer) handleRestartProcess(ctx context.Context, request *mcp.CallToolRequest, args ProcessNameArgs) (*mcp.CallToolResult, any, error) {
	err := s.projectRunner.RestartProcess(args.Name)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("Error restarting process %s: %v", args.Name, err)},
			},
			IsError: true,
		}, nil, nil
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: fmt.Sprintf("Process %s restarted successfully", args.Name)},
		},
	}, map[string]string{"status": "restarted", "process": args.Name}, nil
}

func (s *MCPServer) handleScaleProcess(ctx context.Context, request *mcp.CallToolRequest, args ScaleArgs) (*mcp.CallToolResult, any, error) {
	err := s.projectRunner.ScaleProcess(args.Name, args.Replicas)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("Error scaling process %s to %d replicas: %v", args.Name, args.Replicas, err)},
			},
			IsError: true,
		}, nil, nil
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: fmt.Sprintf("Process %s scaled to %d replicas successfully", args.Name, args.Replicas)},
		},
	}, map[string]interface{}{"status": "scaled", "process": args.Name, "replicas": args.Replicas}, nil
}

func (s *MCPServer) handleGetProcessLogs(ctx context.Context, request *mcp.CallToolRequest, args LogArgs) (*mcp.CallToolResult, any, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	
	endOffset := args.EndOffset
	limit := args.Limit
	if limit == 0 {
		limit = 100
	}

	type logResult struct {
		logs []string
		err  error
	}
	
	resultChan := make(chan logResult, 1)
	go func() {
		logs, err := s.projectRunner.GetProcessLog(args.Name, endOffset, limit)
		resultChan <- logResult{logs: logs, err: err}
	}()
	
	select {
	case result := <-resultChan:
		if result.err != nil {
			return &mcp.CallToolResult{
				Content: []mcp.Content{
					&mcp.TextContent{Text: fmt.Sprintf("Error getting logs for process %s: %v", args.Name, result.err)},
				},
				IsError: true,
			}, nil, nil
		}
		
		logText := ""
		for _, line := range result.logs {
			logText += line + "\n"
		}
		
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: logText},
			},
		}, map[string]interface{}{"process": args.Name, "lines": len(result.logs), "logs": result.logs}, nil
		
	case <-timeoutCtx.Done():
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("Timeout getting logs for process %s", args.Name)},
			},
			IsError: true,
		}, nil, nil
	}
}

func (s *MCPServer) handleGetProjectState(ctx context.Context, request *mcp.CallToolRequest, args ProjectStateArgs) (*mcp.CallToolResult, any, error) {
	state, err := s.projectRunner.GetProjectState(args.WithMemory)
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("Error getting project state: %v. This may indicate configuration loading issues or missing process definitions.", err)},
			},
			IsError: true,
		}, nil, nil
	}

	content, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return &mcp.CallToolResult{
			Content: []mcp.Content{
				&mcp.TextContent{Text: fmt.Sprintf("Error marshaling project state: %v", err)},
			},
			IsError: true,
		}, nil, nil
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: string(content)},
		},
	}, state, nil
}

func (s *MCPServer) handleProjectConfigResource(ctx context.Context, request *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	projectName, err := s.projectRunner.GetProjectName()
	if err != nil {
		return nil, fmt.Errorf("failed to get project name: %w", err)
	}

	projectState, err := s.projectRunner.GetProjectState(false)
	if err != nil {
		return nil, fmt.Errorf("failed to get project state: %w", err)
	}

	config := map[string]interface{}{
		"name":  projectName,
		"state": projectState,
	}

	content, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal project config: %w", err)
	}

	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{
			{
				URI:      request.Params.URI,
				MIMEType: "application/json",
				Text:     string(content),
			},
		},
	}, nil
}

func (s *MCPServer) handleProcessStatesResource(ctx context.Context, request *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	states, err := s.projectRunner.GetProcessesState()
	if err != nil {
		return nil, fmt.Errorf("failed to get processes state: %w", err)
	}

	content, err := json.MarshalIndent(states, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal processes state: %w", err)
	}

	return &mcp.ReadResourceResult{
		Contents: []*mcp.ResourceContents{
			{
				URI:      request.Params.URI,
				MIMEType: "application/json",
				Text:     string(content),
			},
		},
	}, nil
}
