import Config

config :elsa_fi,
  brokers: [localhost: 9092],
  divo: "compose.yml",
  divo_wait: [dwell: 700, max_tries: 50]

config :logger,
  handle_sasl_reports: false,
  level: :warn
