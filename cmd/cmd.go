package cmd

import (
	"context"

	"github.com/urfave/cli"

	"github.com/oliverkra/gqm/cmd/example"
	"github.com/oliverkra/gqm/cmd/server"
)

func Run(args []string) error {
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "grpc",
			EnvVar: "GQM_GRPC",
			Value:  ":7070",
			Usage:  "GRPC listen address",
		},
	}
	app.Commands = []cli.Command{
		cli.Command{
			Name: "server",
			Action: func(ctx *cli.Context) error {
				return server.Run(context.Background(), server.Options{
					GrpcAddr: ctx.GlobalString("grpc"),
				})
			},
		},
		cli.Command{
			Name:  "examples",
			Flags: []cli.Flag{},
			Subcommands: []cli.Command{
				cli.Command{
					Name: "pub",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "q,queue",
							Value: "gqm.test",
							Usage: "Queue name",
						},
						cli.IntFlag{
							Name:  "c,count",
							Value: 1,
							Usage: "Count messages",
						},
					},
					Action: func(ctx *cli.Context) error {
						return example.RunPub(example.PubOptions{
							GrpcAddr: ctx.GlobalString("grpc"),
							Queue:    ctx.String("queue"),
							Count:    ctx.Int("count"),
						})
					},
				},
				cli.Command{
					Name: "sub",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "q,queue",
							Value: "gqm.test",
							Usage: "Queue name",
						},
						cli.StringFlag{
							Name:  "g,group",
							Usage: "Group name",
						},
					},
					Action: func(ctx *cli.Context) error {
						return example.RunSub(example.SubOptions{
							GrpcAddr: ctx.GlobalString("grpc"),
							Queue:    ctx.String("queue"),
							Group:    ctx.String("group"),
						})
					},
				},
			},
		},
	}
	return app.Run(args)
}
