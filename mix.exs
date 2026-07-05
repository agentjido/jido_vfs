defmodule Jido.VFS.MixProject do
  use Mix.Project

  @version "1.0.0"
  @source_url "https://github.com/agentjido/jido_vfs"
  @description "A filesystem abstraction for Elixir with adapters for Local, S3, Git, GitHub, ETS, and InMemory storage."

  def project do
    [
      app: :jido_vfs,
      version: @version,
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),

      # Documentation
      name: "Jido.VFS",
      description: @description,
      source_url: @source_url,
      homepage_url: @source_url,
      package: package(),
      docs: docs(),

      # Test Coverage
      test_coverage: [
        tool: ExCoveralls,
        summary: [threshold: 80]
      ],

      # Dialyzer
      dialyzer: [
        plt_local_path: "priv/plts/project.plt",
        plt_core_path: "priv/plts/core.plt",
        ignore_warnings: ".dialyzer_ignore.exs"
      ]
    ]
  end

  def cli do
    [
      preferred_envs: [
        coveralls: :test,
        "coveralls.github": :test,
        "coveralls.html": :test
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Jido.VFS.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      # Runtime dependencies
      {:ex_aws, "~> 2.7"},
      {:ex_aws_s3, "~> 2.2"},
      {:sweet_xml, "~> 0.6"},
      {:jason, "~> 1.0"},
      {:eternal, "~> 1.2.2"},
      {:splode, "~> 0.3.0"},
      {:git_cli, "~> 0.3.0"},
      {:req, "~> 0.6"},

      # Dev/Test dependencies
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:doctor, "~> 0.21", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:excoveralls, "~> 0.18", only: [:dev, :test]},
      {:mimic, "~> 2.0", only: :test},
      {:minio_server, "~> 0.4.0", only: [:dev, :test]},
      {:mix_test_watch, "~> 1.0", only: [:dev, :test], runtime: false},
      {:stream_data, "~> 1.0", only: [:dev, :test]},

      # Code generation
      {:igniter, "~> 0.7", optional: true}
    ]
  end

  defp aliases do
    [
      setup: ["deps.get"],
      test: "test --exclude flaky",
      docs: "docs --formatter html",
      q: ["quality"],
      quality: [
        "format --check-formatted",
        "compile --warnings-as-errors",
        "credo --min-priority higher",
        "dialyzer",
        "doctor --raise"
      ],
      "release.ready": [
        "deps.unlock --check-unused",
        "quality",
        "cmd env MIX_ENV=test mix coveralls --include integration",
        "docs",
        "cmd mix hex.audit",
        "cmd mix hex.build"
      ]
    ]
  end

  defp package do
    [
      files: ["lib", "docs", "mix.exs", "README.md", "LICENSE.md", "CHANGELOG.md", "usage-rules.md"],
      maintainers: ["AgentJido"],
      licenses: ["Apache-2.0"],
      links: %{
        "Changelog" => "https://hexdocs.pm/jido_vfs/changelog.html",
        "Discord" => "https://agentjido.xyz/discord",
        "Documentation" => "https://hexdocs.pm/jido_vfs",
        "GitHub" => @source_url,
        "Website" => "https://agentjido.xyz"
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      extras: [
        "README.md",
        "LICENSE.md",
        "CHANGELOG.md",
        "CONTRIBUTING.md",
        "docs/jido-agent-integration.md",
        "docs/adapter-onboarding-checklist.md"
      ],
      groups_for_modules: [
        Adapters: [
          ~r/^Jido\.VFS\.Adapter\./
        ],
        Stat: [
          ~r/^Jido\.VFS\.Stat\./
        ],
        Errors: [
          ~r/^Jido\.VFS\.Errors/
        ]
      ]
    ]
  end
end
