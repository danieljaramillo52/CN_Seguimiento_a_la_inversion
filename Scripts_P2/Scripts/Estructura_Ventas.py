import pandas as pd
from Transformation_functions import Renombrar_columnas_con_diccionario, Eliminar_acentos, Formatear_primera_letra_mayuscula
from Lectura_insumos import base_estructura_ventas
from config_constans import (
    COLS_ESTRUCTURA_VENTAS_SELECCIONAR,
    COLS_EV_FIGURA_COMERCIAL,
    NOM_VENDEDOR_1,
    COD_JEFE_VENTAS,
    NOM_JEFE_VENTAS,
    COD_VENDEDOR,
    NOM_FIGURA_COMER,
    COD_FIGURA_COMER,
    COLS_ESTRUCTURA_VENTAS_RENOMBRAR,
)

base_ev_Nom_figura_comer = Eliminar_acentos(
    base_estructura_ventas[COLS_EV_FIGURA_COMERCIAL]
)

base_driver_estructura_venta = Formatear_primera_letra_mayuscula(dataframe=base_ev_Nom_figura_comer, columna=NOM_JEFE_VENTAS)

base_driver_estructura_venta = Formatear_primera_letra_mayuscula(
  dataframe=base_ev_Nom_figura_comer, columna=NOM_VENDEDOR_1 
)

# Obtener las columnas "Cod. Vendedor" y "Nombre Vendedor 1"
df1 = base_ev_Nom_figura_comer[[COD_VENDEDOR, NOM_VENDEDOR_1]]

# Obtener las columnas ["Cod. Jefe Vtas y  "Nombre Jefe Ventas"]
df2 = base_ev_Nom_figura_comer[[COD_JEFE_VENTAS, NOM_JEFE_VENTAS]].rename(
    columns={
        COD_JEFE_VENTAS: COD_VENDEDOR,
        NOM_JEFE_VENTAS: NOM_VENDEDOR_1,
    }
)

# Concatenar las columnas ["Cod. Jefe Vtas y  "Nombre Jefe Ventas"]
base_nom_figura_comer = pd.concat([df1, df2], axis=0, ignore_index=True)

# Renombrar columnas necesarias.
base_nom_figura_comer_rename = base_nom_figura_comer.rename(
    columns={
        COD_VENDEDOR: COD_FIGURA_COMER,
        NOM_VENDEDOR_1: NOM_FIGURA_COMER,
    },
)

# Estructura de Ventas en los drivers.
base_driver_estructura_venta = base_estructura_ventas.drop_duplicates(
    subset=COLS_ESTRUCTURA_VENTAS_SELECCIONAR
)
base_driver_estructura_venta_rename = Renombrar_columnas_con_diccionario(
    base=base_driver_estructura_venta, cols_to_rename=COLS_ESTRUCTURA_VENTAS_RENOMBRAR
)
