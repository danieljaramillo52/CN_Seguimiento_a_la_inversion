from Transformation_functions import (
    filtrar_por_valores,
    Filtrar_por_valores_excluidos,
    Agregar_columna_constante,
    Renombrar_columnas_con_diccionario,
)
from Lectura_insumos import base_socios_seleccionada

import config_constans as cc

ATENCION = cc.ATENCION
SUBCANAL = cc.SUBCANAL
CATEGORIA = cc.CATEGORIA
SOCIOS = cc.SOCIOS
FIL_SUB_CANAL_SOCIOS = cc.FIL_SUB_CANAL_SOCIOS
REEMPLAZOS_SOCIOS_SUBCANAL = cc.REEMPLAZOS_SOCIOS_SUBCANAL
SOCIOS_RENOMBRAR = cc.SOCIOS_RENOMBRAR

base_socios_filtrada_atencion = filtrar_por_valores(
    df=base_socios_seleccionada, columna=ATENCION, valores_filtrar="Directa"
)
base_socios_filtrada_categoria = Filtrar_por_valores_excluidos(
    df=base_socios_seleccionada,
    columna=CATEGORIA,
    valores_excluir="Plus Red - Socios",
)
# Eliminar columna categoria
base_socios_filtrada_categoria = base_socios_filtrada_categoria.drop(
    "Categoria", axis=1
)
base_socios_filtrada_total = filtrar_por_valores(
    base_socios_filtrada_categoria, SUBCANAL, FIL_SUB_CANAL_SOCIOS
)
base_socios_filtrada_total = Agregar_columna_constante(
    dataframe=base_socios_filtrada_total, nombre_col_cols=SOCIOS, valor_constante="Si"
)

# Extraemos la columna subcanal
subcanal_socios = base_socios_filtrada_total[SUBCANAL]

# Hacemos los reemplazos correspondientes en la columna subcanal.
subcanal_socios = subcanal_socios.replace(REEMPLAZOS_SOCIOS_SUBCANAL)

# Actualizamos la columna subcanal en el dataframe.
base_socios_filtrada_total.loc[:, SUBCANAL] = subcanal_socios

# Renombrar cols socios.
base_socios_filtrada_total_rename = Renombrar_columnas_con_diccionario(
    base=base_socios_filtrada_total, cols_to_rename=SOCIOS_RENOMBRAR
)
