defmodule Elsa.MixProject do
  use Mix.Project

  @version "0.0.1"
  @github "https://github.com/simplifi/elsa_fi"

  def project do
    [
      app: :elsa_fi,
      name: "Elsa.fi",
      version: @version,
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      deps: deps(),
      homepage: @github,
      docs: docs(),
      elixirc_paths: elixirc_paths(Mix.env()),
      test_paths: test_paths(Mix.env()),
      dialyzer: [plt_file: {:no_warn, ".plt/#{System.version()}.plt"}]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:brod, "~> 3.16.5"},
      {:patiently, "~> 0.2", only: [:dev, :test, :integration]},
      {:divo, "~> 2.0", only: [:dev, :test, :integration], override: true},
      {:divo_kafka, "~> 1.0", only: [:dev, :test, :integration]},
      {:mock, "~> 0.3", only: [:dev, :test]},
      {:checkov, "~> 1.0", only: [:test, :integration]},
      {:ex_doc, "~> 0.29", only: [:dev]},
      {:dialyxir, "~> 1.3", only: [:dev], runtime: false}
    ]
  end

  defp elixirc_paths(env) when env in [:test, :integration], do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp test_paths(:integration), do: ["test/integration"]
  defp test_paths(_), do: ["test/unit"]

  defp package do
    [
      maintainers: ["Simpli.fi Development Team"],
      licenses: ["Apache 2.0"],
      links: %{"GitHub" => @github}
    ]
  end

  defp description do
    "Elsa is a full-featured Kafka library written in Elixir and extending the :brod library with additional support from the :kafka_protocol Erlang libraries to provide capabilities not available in :brod. (Simpli.fi fork)"
  end

  defp docs do
    [
      source_ref: "v#{@version}",
      source_url: @github,
      extras: ["README.md"],
      source_url_pattern: "#{@github}/blob/master/%{path}#L%{line}"
    ]
  end
end
