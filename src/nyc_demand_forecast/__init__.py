"""nyc-demand-forecast: orchestrated analytics-engineering + forecasting pipeline."""


def main() -> int:
    from nyc_demand_forecast.cli import main as cli_main

    return cli_main()
