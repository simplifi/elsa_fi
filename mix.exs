defmodule Elsa.MixProject do
  use Mix.Project

  @github "https://github.com/simplifi/elsa_fi"

  def project do
    [
      app: :elsa_fi,
      name: "Elsa.fi",
      version: version(),
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      deps: deps(),
      homepage: @github,
      docs: docs(),
      elixirc_paths: elixirc_paths(Mix.env()),
      test_paths: test_paths(Mix.env()),
      dialyzer: dialyzer()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      # Pointing to a fork until https://github.com/kafka4beam/brod/pull/625 gets merged in 
      {:brod, git: "https://github.com/simplifi/brod", ref: "7304dc33b524fe32aa620890ef773ebc3b0928c3"},
      {:patiently, "~> 0.2", only: [:dev, :test, :integration]},
      {:divo, "~> 2.0", only: [:dev, :test, :integration], override: true},
      {:divo_kafka, "~> 1.0", only: [:dev, :test, :integration]},
      {:mock, "~> 0.3", only: [:dev, :test]},
      {:checkov, "~> 1.0", only: [:test, :integration]},
      {:ex_doc, "~> 0.29", only: [:dev]},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
      {:credo, "~> 1.7", only: :dev}
    ]
  end

  defp dialyzer do
    [
      # include all direct dependencies
      plt_add_deps: :apps_direct,
      # add any indirect dependencies that are still used directly in our code
      plt_add_apps: [
        :kafka_protocol
      ],
      flags: [
        :unmatched_returns,
        :error_handling
      ],
      list_unused_filters: true
    ]
  end

  defp elixirc_paths(env) when env in [:test, :integration], do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp test_paths(:integration), do: ["test/integration"]
  defp test_paths(_), do: ["test/unit"]

  defp package do
    [
      maintainers: ["Simpli.fi Development Team"],
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @github}
    ]
  end

  defp description do
    "Elsa is a full-featured Kafka library written in Elixir and extending the :brod library with additional support from the :kafka_protocol Erlang libraries to provide capabilities not available in :brod. (Simpli.fi fork)"
  end

  # Auto version stamp, a la CoMix.
  defp version do
    default_version = "0.0.1-tagless"

    case :file.consult("hex_metadata.config") do
      # Use version from hex_metadata when we're a package
      {:ok, data} ->
        {"version", version} = List.keyfind(data, "version", 0)
        version

      # Otherwise, use git version
      _ ->
        case System.cmd("git", ["describe", "--tags"]) do
          {"v" <> version, 0} -> String.trim(version)
          _ -> default_version
        end
    end
  end

  defp docs do
    [
      source_ref: "v#{version()}",
      source_url: @github,
      extras: ["README.md"],
      source_url_pattern: "#{@github}/blob/master/%{path}#L%{line}"
    ]
  end
end
