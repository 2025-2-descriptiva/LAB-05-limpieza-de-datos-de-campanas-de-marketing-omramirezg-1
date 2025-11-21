"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


"""
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """



import os
import zipfile
from pathlib import Path

import pandas as pd


def _yes_to_one(value):
    return 1 if str(value).lower() == "yes" else 0


def _success_to_one(value):
    return 1 if str(value).lower() == "success" else 0


def _month_to_number(month_str):
    mapa_meses = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4,
        "may": 5, "jun": 6, "jul": 7, "aug": 8,
        "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    return mapa_meses.get(str(month_str).lower(), 1)


def clean_campaign_data():
    carpeta_entrada = Path("files/input")
    carpeta_salida = Path("files/output")
    carpeta_salida.mkdir(parents=True, exist_ok=True)

    # 1. Leer y concatenar todos los CSV contenidos en archivos .zip
    marcos = []

    for zip_path in carpeta_entrada.glob("*.zip"):
        with zipfile.ZipFile(zip_path, mode="r") as archivo_zip:
            for nombre in archivo_zip.namelist():
                if nombre.endswith(".csv"):
                    with archivo_zip.open(nombre) as manejador:
                        df_parcial = pd.read_csv(manejador, sep=",")
                        marcos.append(df_parcial)

    if not marcos:
        raise FileNotFoundError(
            "No se encontraron archivos .csv dentro de los archivos .zip en files/input."
        )

    datos = pd.concat(marcos, ignore_index=True)

    # 2. Construir client.csv
    columnas_client = [
        "client_id",
        "age",
        "job",
        "marital",
        "education",
        "credit_default",
        "mortgage",
    ]
    client = datos[columnas_client].copy()

    client["job"] = (
        client["job"]
        .str.replace(".", "", regex=False)
        .str.replace("-", "_", regex=False)
    )

    client["education"] = (
        client["education"]
        .str.replace(".", "_", regex=False)
        .replace("unknown", pd.NA)
    )

    client["credit_default"] = client["credit_default"].apply(_yes_to_one)
    client["mortgage"] = client["mortgage"].apply(_yes_to_one)

    client.to_csv(carpeta_salida / "client.csv", index=False)

    # 3. Construir campaign.csv
    columnas_campaign = [
        "client_id",
        "number_contacts",
        "contact_duration",
        "previous_campaign_contacts",
        "previous_outcome",
        "campaign_outcome",
        "month",
        "day",
    ]
    campaign = datos[columnas_campaign].copy()

    campaign["previous_outcome"] = campaign["previous_outcome"].apply(
        _success_to_one
    )
    campaign["campaign_outcome"] = campaign["campaign_outcome"].apply(
        _yes_to_one
    )

    campaign["last_contact_date"] = campaign.apply(
        lambda fila: f"2022-{_month_to_number(fila['month']):02d}-{int(fila['day']):02d}",
        axis=1,
    )

    campaign = campaign.drop(columns=["month", "day"])
    campaign.to_csv(carpeta_salida / "campaign.csv", index=False)

    # 4. Construir economics.csv
    columnas_economics = [
        "client_id",
        "cons_price_idx",
        "euribor_three_months",
    ]
    economics = datos[columnas_economics].copy()
    economics.to_csv(carpeta_salida / "economics.csv", index=False)


if __name__ == "__main__":
    clean_campaign_data()
