
# weather.py
import typer
from enum import Enum



app = typer.Typer()


@app.callback()
def callback():
    """
    Awesome Portal Gun
    """


@app.command()
def shoot():
    """
    Shoot the portal gun
    """
    typer.echo("Shooting portal gun")


@app.command()
def load():
    """
    Load the portal gun
    """
    typer.echo("Loading portal gun")


# """
# Creating a Mars Weather CLI.
# https://patternite.com/patterns/0d7aab4f39/write-better-python-clis-typer
# """
# app = typer.Typer()
# pressure_app = typer.Typer()
# temperature_app = typer.Typer()
# app.add_typer(pressure_app, name="pressure")
# app.add_typer(temperature_app, name="temperature")


# # We refer to Martian days as "sols"
# import requests
# URL = "https://api.nasa.gov/insight_weather/?api_key=DEMO_KEY&feedtype=json&ver=1.0"
# response = requests.get(URL).json()
# # filter non-weather data
# sols = list(filter(lambda x: x.isnumeric(), response.keys()))
# # most recent sol is the last element of `sols`
# current_sol_data = response[sols[-1]]


# # We define an Enum which specifies the valid options for the Mars hemisphere
# class Hemisphere(Enum):
#     north = "north"
#     south = "south"

# # By passing in our `Hemisphere` Enum, Typer will ensure either "north" or "south"
# # is passed as a CLI option, or the command will fail. Running --help will also show
# # "north" and "south" as the valid options.


# @app.command()
# def season(hemisphere: Hemisphere):
#     if hemisphere.value == "north":
#         typer.echo(current_sol_data["Northern_season"])
#     else:
#         typer.echo(current_sol_data["Southern_season"])


# # Running this callback ensures we can check that the measurements exist before
# # trying to access them
# @pressure_app.callback()
# def pressure_main():
#     if "PRE" not in current_sol_data:
#         typer.echo("Pressure measurements not available for this sol.")
#         raise typer.Exit()


# @pressure_app.command()
# def maximum():
#     typer.echo(current_sol_data["PRE"]["mx"])


# @pressure_app.command()
# def minimum():
#     typer.echo(current_sol_data["PRE"]["mn"])


# @temperature_app.callback()
# def temperature_main():
#     if "AT" not in current_sol_data:  # "AT" stands for "atmospheric temperature"
#         typer.echo("Temperature measurements not available for this sol.")
#         raise typer.Exit()


# @temperature_app.command()
# def maximum():
#     typer.echo(current_sol_data["AT"]["mx"])


# @temperature_app.command()
# def minimum():
#     typer.echo(current_sol_data["AT"]["mn"])
