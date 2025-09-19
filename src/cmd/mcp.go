package cmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/f1bonacc1/process-compose/src/mcp"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var mcpCmd = &cobra.Command{
	Use:   "mcp",
	Short: "MCP (Model Context Protocol) server commands",
	Long:  `Commands for running process-compose as an MCP server`,
}

var mcpServeCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start MCP server",
	Long: `Start process-compose as an MCP server.
The server will provide tools and resources for process management via MCP protocol.`,
	Run: func(cmd *cobra.Command, args []string) {
		runMCPServer(args)
	},
}

var (
	mcpStdio = false
	mcpHTTP  = false
	mcpPort  = 9090
)

func init() {
	rootCmd.AddCommand(mcpCmd)
	mcpCmd.AddCommand(mcpServeCmd)

	mcpServeCmd.Flags().BoolVar(&mcpStdio, "stdio", true, "Use stdio transport (default)")
	mcpServeCmd.Flags().BoolVar(&mcpHTTP, "http", false, "Use HTTP transport")
	mcpServeCmd.Flags().IntVar(&mcpPort, "port", 9090, "Port for HTTP transport")

	mcpServeCmd.Flags().StringArrayP("config", "f", []string{"process-compose.yaml"}, "path to config files to load")
	mcpServeCmd.Flags().StringArrayP("env", "e", []string{".env"}, "path to env files to load")
	mcpServeCmd.Flags().Bool("disable-dotenv", false, "disable .env file loading")
}

func runMCPServer(args []string) {
	runner := getProjectRunner(args, false, "", []string{})
	if runner == nil {
		log.Fatal().Msg("Failed to initialize project runner")
		return
	}

	mcpServer, err := mcp.NewMCPServer(runner)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create MCP server")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	processReady := make(chan bool, 1)
	go func() {
		log.Info().Msg("Starting processes in background...")
		if err := runner.Run(); err != nil {
			log.Error().Err(err).Msg("Process runner failed")
		}
		time.Sleep(2 * time.Second)
		processReady <- true
	}()

	select {
	case <-processReady:
		log.Info().Msg("Processes initialized, starting MCP server...")
	case <-time.After(10 * time.Second):
		log.Warn().Msg("Process initialization timeout, starting MCP server anyway...")
	}

	go func() {
		<-sigChan
		log.Info().Msg("Received interrupt signal, shutting down MCP server...")
		_ = runner.ShutDownProject()
		cancel()
	}()

	if mcpHTTP {
		log.Info().Msgf("Starting MCP server with HTTP transport on port %d", mcpPort)
		log.Fatal().Msg("HTTP transport not yet implemented, use --stdio")
	} else {
		log.Info().Msg("Starting MCP server with stdio transport")
		if err := mcpServer.ServeStdio(ctx); err != nil {
			if err == context.Canceled {
				log.Info().Msg("MCP server stopped")
			} else {
				log.Fatal().Err(err).Msg("MCP server failed")
			}
		}
	}
}
