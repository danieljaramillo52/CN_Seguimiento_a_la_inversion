import pandas as pd
import numpy as np
from typing import Union, List, Any
from time import time
from loguru import logger
from unidecode import unidecode
from General_Functions import (
    Registro_tiempo,
    extraer_sublista_hasta_elemento,
    verificar_existencia_columnas,
)


def invertir_diccionario(diccionario_original):
    """
    Invierte las llaves y valores de un diccionario.

    Parámetros:
    - diccionario_original (dict): El diccionario a invertir.

    Retorna:
    - dict: El diccionario invertido.

    Ejemplo:
    >>> diccionario_original = {
    ...     "Ene": "01",
    ...     "Feb": "02",
    ...     "Mar": "03",
    ... }
    >>> invertir_diccionario(diccionario_original)
    {
        '01': 'Ene',
        '02': 'Feb',
        '03': 'Mar',
    }
    """
    try:
        if not isinstance(diccionario_original, dict):
            raise ValueError("El argumento no es un diccionario.")

        diccionario_invertido = {
            valor: llave for llave, valor in diccionario_original.items()
        }
        logger.success("Diccionario invertido con éxito.")
        return diccionario_invertido
    except Exception as e:
        logger.error("Error al invertir el diccionario: {}".format(str(e)))
        raise ValueError("Error al invertir el diccionario: {}".format(str(e)))


@Registro_tiempo
def Concatenate_dataframes_from_dict(data_dict: dict, bases_concatenadas={}) -> dict:
    """
    Concatena DataFrames desde un diccionario de listas de DataFrames.

    Args
    data_dict (dict): Un diccionario donde las claves representan tipos de consultas y los valores son listas de DataFrames.

    Returns:
    concatenated_dfs (dict): Un diccionario que contiene DataFrames concatenados para cada tipo de consulta.

    *Nota: Cuando usamos la paralabra "consulta", se hace alusión a un archivo de excel con extensiones .xlsx ó xlsm, o  archivo de texto plano .csv
    """
    try:
        logger.info(f"Proceso de concatenación de dataframes.")
        for key, lista_dataframes in data_dict.items():
            # Concatena los DataFrames del diccionario en un DataFrame
            df_concatenado = pd.concat(lista_dataframes, ignore_index=True)
            bases_concatenadas[key] = df_concatenado
    except Exception:
        logger.critical("Proceso de concatenación fallido {e}")
        raise Exception

    bases_concatenadas[key] = df_concatenado

    return bases_concatenadas


@Registro_tiempo
def concatenar_dataframes(df_list: list[pd.DataFrame]):
    """
    Concatena una lista de DataFrames.

    Args:
        df_list: Lista de DataFrames a concatenar.

    Returns:
        Un DataFrame concatenado.
    """
    try:
        if len(df_list) != 1:
            logger.info("Inicio concatenacion de dataframes")
            concatenados = pd.concat(df_list, ignore_index=True)
            logger.success("se concatenaron los dataframes correctamente")
            return concatenados
        else:
            return df_list[0]
    except Exception as e:
        logger.critical(e)
        raise e


def Eliminar_filas_con_cadena(df: pd.DataFrame, columna: str, cadena: str):
    """
    Elimina todas las filas que contengan una palabra específica en una columna del DataFrame.

    Args:
        - df_name (str): Nombre del DataFrame.
        - columna (str): Nombre de la columna en la que se realizará la búsqueda.
        - cadena (str): Palabra específica que se utilizará como criterio de eliminación.

    Returns:
        pd.DataFrame: Nuevo DataFrame sin las filas que contienen la palabra especificada.
    """
    try:
        # Eliminar filas que contengan la palabra en la columna especificada
        df_filtrado = df[
            ~df[columna].str.contains(rf"\b{cadena}\b", case=False, regex=True)
        ]

        # Registrar información sobre las filas eliminadas
        logger.info(
            f"Filas que contienen '{cadena}' en la columna '{columna}' eliminadas con éxito."
        )

        return df_filtrado

    except KeyError as ke:
        # Registrar un error específico si la columna no existe
        logger.critical(f"Error al eliminar filas: {str(ke)}")
        # Propagar la excepción para que el usuario sea consciente del problema
        raise ke


def Eliminar_columnas(df: pd.DataFrame, columnas_a_eliminar: list) -> pd.DataFrame:
    """
    Elimina las columnas especificadas de un DataFrame de pandas.

    Args:
        - df (pd.DataFrame): El DataFrame de pandas original.
        - columnas_a_eliminar (list): Lista de nombres de columnas a eliminar.

    Returns:
        pd.DataFrame: Un nuevo DataFrame sin las columnas especificadas.

    Example:
        >>> df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6], 'C': [7, 8, 9]})
        >>> columnas_a_eliminar = ['B', 'C']
        >>> nuevo_df = eliminar_columnas(df, columnas_a_eliminar)
        >>> print(nuevo_df)
           A
        0  1
        1  2
        2  3
    """
    try:
        # Eliminar las columnas del DataFrame
        df_resultado = df.drop(columns=columnas_a_eliminar)

        # Registrar información sobre las columnas eliminadas
        logger.info(f"Columnas {columnas_a_eliminar} eliminadas con éxito.")

        return df_resultado
    except Exception as e:
        # Registrar un error crítico si ocurre una excepción
        logger.critical(f"Error al eliminar columnas: {str(e)}")
        # Propagar la excepción para que el usuario sea consciente del problema
        raise e


def Eliminar_primeras_n_filas_dict(data_dict: dict, n: int) -> dict:
    """
    Elimina las primeras n filas de cada DataFrame en las listas correspondientes a los valores de un diccionario.

    Args:
    data_dict (dict): Un diccionario donde las claves representan nombres de consultas y los valores son listas de DataFrames.
    n (int): Número de filas a eliminar.

    *Nota: Cuando usamos la paralabra "consulta", se hace alusión a un archivo de excel con extensiones .xlsx ó xlsm, o  archivo de texto plano .csv

    Returns:
    data_dict (dict): Un nuevo diccionario con las primeras n filas eliminadas de cada DataFrame.
    """
    try:
        logger.info(f"Eliminación de filas de todos los dataframes: ")

        # Crear una copia del diccionario para no modificar el original
        new_data_dict = data_dict.copy()

        for key, lista_dataframes in new_data_dict.items():
            for i, df in enumerate(lista_dataframes):
                new_data_dict[key][i] = df.iloc[n:]  # Elimina las primeras n filas
        logger.success("Eliminación de filas completa.")
    except Exception as e:
        logger.critical("Proceso de eliminación de filas fallido.")
        raise e
    return new_data_dict


def obtener_valores_unicos_sin_nulos(columna: pd.Series) -> List[Any]:
    """
    Obtiene los valores únicos de una columna de un DataFrame, sin contar los nulos.

    Args:
        columna: La columna de la que se desea obtener los valores únicos.

    Returns:
        Una lista con los valores únicos de la columna.

    Raises:
        TypeError: Si la columna no es una serie.

    Raises:
        Exception: Si se produce cualquier otro error.
    """

    # Validamos que la columna sea una serie.

    if not isinstance(columna, pd.Series):
        raise TypeError(
            f"La columna debe ser una serie, pero se recibió un {type(columna)}."
        )

    # Capturamos cualquier otro error que pueda ocurrir.

    try:
        columna_sin_nulos = columna.dropna()
        valores_unicos = columna_sin_nulos.unique()
    except Exception as e:
        logger.critical(e)
        raise e

    # Registramos un mensaje de información.

    logger.info(
        f"Se obtuvieron los valores únicos de la columna {columna.name}: {valores_unicos}."
    )

    return valores_unicos


def Eliminar_primeras_n_filas(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    Elimina las primeras n filas de un DataFrame.

    Args:
    df (pd.DataFrame): El DataFrame del que se desea eliminar las primeras n filas.
    n (int): Número de filas a eliminar.

    Returns:
    df (pd.DataFrame): El DataFrame con las primeras n filas eliminadas.
    """

    try:
        logger.info(f"Eliminación de filas del dataframe: ")

        # Elimina las primeras n filas
        df = df.iloc[n:]

        logger.success("Eliminación de filas completa.")
    except Exception as e:
        logger.critical("Proceso de eliminación de filas fallido.")
        raise e
    return df


@Registro_tiempo
def Renombrar_columnas_con_diccionario(
    base: pd.DataFrame, cols_to_rename: dict
) -> pd.DataFrame:
    """Funcion que toma un diccionario con keys ( nombres actuales ) y values (nuevos nombres) para remplazar nombres de columnas en un dataframe.
    Args:
        base: dataframe al cual se le harán los remplazos
        cols_to_rename: diccionario con nombres antiguos y nuevos
    Result:
        base_renombrada: Base con las columnas renombradas.
    """
    base_renombrada = None

    try:
        base_renombrada = base.rename(columns=cols_to_rename, inplace=False)
        logger.success("Proceso de renombrar columnas satisfactorio: ")
    except Exception:
        logger.critical("Proceso de renombrar columnas fallido.")
        raise Exception

    return base_renombrada


def obtener_nombre_variable(valor_variable, contexto):
    """_Función que toma una variable en especifio. y retorna el nombre especifico
    de la varaible pasada como argumento La varaible en un contexto especifico Local ó global.

    Args:
        valor_variable : pasa una varaible en cuestión y nos devuelve el nombre en especifico asignado a esta variable
        contexto: Local o global de la varaible en cuestión.  Generalmente => Locals.

    Returns:
        nombre: Nombre que se le asigno a la varaible declarada.
    """
    for nombre, valor in contexto.items():
        if valor is valor_variable:
            return nombre
    return None


@Registro_tiempo
def Reemplazar_columna_en_funcion_de_otra(
    df: pd.DataFrame,
    nom_columna_a_reemplazar: str,
    nom_columna_de_referencia: str,
    mapeo: dict,
) -> pd.DataFrame:
    """
    Reemplaza los valores en una columna en función de los valores en otra columna en un DataFrame.

    Args:
        df (pandas.DataFrame): El DataFrame en el que se realizarán los reemplazos.
        columna_a_reemplazar (str): El nombre de la columna que se reemplazará.
        columna_de_referencia (str): El nombre de la columna que se utilizará como referencia para el reemplazo.
        mapeo (dict): Un diccionario que mapea los valores de la columna de referencia a los nuevos valores.

    Returns:
        pandas.DataFrame: El DataFrame actualizado con los valores reemplazados en la columna indicada.
    """
    try:
        logger.info(f"Inicio de remplazamiento de datos en {nom_columna_a_reemplazar}")
        df[nom_columna_a_reemplazar] = np.where(
            df[nom_columna_de_referencia].isin(mapeo.keys()),
            df[nom_columna_de_referencia].map(mapeo),
            df[nom_columna_a_reemplazar],
        )
        logger.success(
            f"Proceso de remplazamiento en {nom_columna_a_reemplazar} exitoso"
        )
    except Exception as e:
        logger.critical(
            f"Proceso de remplazamiento de datos en {nom_columna_a_reemplazar} fallido."
        )
        raise e

    return df


def Reemplazar_valores_misma_columna(
    df: pd.DataFrame, columna: str, diccionario_mapeo: dict
):
    """
    Reemplaza los valores en la columna especificada de un DataFrame según un diccionario de mapeo.

    Args:
    - df (pd.DataFrame): El DataFrame a modificar.
    - columna (str): El nombre de la columna que se va a reemplazar.
    - diccionario_mapeo (dict): Un diccionario que define la relación de mapeo de valores antiguos a nuevos.

    Returns:
    - pd.DataFrame: El DataFrame modificado con los valores de la columna especificada reemplazados.

    - TypeError: Si 'df' no es un DataFrame de pandas o 'diccionario_mapeo' no es un diccionario.
    - KeyError: Si la 'columna' especificada no se encuentra en el DataFrame.

    """
    try:
        # Verificar si la entrada es un DataFrame de pandas
        if not isinstance(df, pd.DataFrame):
            raise TypeError("El argumento 'df' debe ser un DataFrame de pandas.")

        # Verificar si la columna especificada existe en el DataFrame
        if columna not in df.columns:
            raise KeyError(f"Columna '{columna}' no encontrada en el DataFrame.")

        # Verificar si el diccionario de mapeo es un diccionario
        if not isinstance(diccionario_mapeo, dict):
            raise TypeError("'diccionario_mapeo' debe ser un diccionario.")

        # Realizar el reemplazo según el diccionario de mapeo
        df.loc[:, columna] = df[columna].replace(diccionario_mapeo)

        # Registrar mensaje de éxito
        logger.success(
            f"Valores de la columna '{columna}' reemplazados según el diccionario de mapeo."
        )

        return df

    except Exception as e:
        # Registrar mensaje crítico con detalles del tipo de error
        logger.critical(
            f"Error durante el reemplazo de valores en la columna. Tipo de error: {type(e).__name__}. Detalles: {str(e)}"
        )
        return None


def Asignar_col_nula(
    dict_consultas: dict, nom_consulta: str, list_columnas: list
) -> pd.DataFrame:
    """Funcion que toma una consulta de un diccionario, y a todas sus hojas les agrega una lista
    de columnas con valores nulos y nombres especificos.

    *Nota: Cuando usamos la paralabra "consulta", se hace alusión a un archivo de excel con extensiones .xlsx ó xlsm, o  archivo de texto plano .csv

    Args:
        dict_consultas (dict): diccionario de donde salen las consultas
        nom_consulta (str): clave del diccionario para la consutla especifica
        list_columnas (list): lista que contiene los nombre de las columnas a agregar.

    Returns:
        pd.DataFrame: dataframe modificado con las nuevas columnas agregadas.
    """

    try:
        logger.info(
            f"Proceso para agregar las columnas {list_columnas} en la consulta {nom_consulta} iniciado:"
        )
        for cada_consulta in dict_consultas[nom_consulta]:
            for cada_columna in list_columnas:
                cada_consulta[cada_columna] = np.nan
        logger.success("Columnas asignadas con exito")
    except Exception as e:
        logger.critical(
            f"Proceso de agregar las columnas {list_columnas} en la consulta {nom_consulta} fallido: "
        )
        raise e
    return dict_consultas  # Devuelve el diccionario modificado


def duplicar_columna(
    df: pd.DataFrame, columna_a_duplicar: str, columna_nueva: str
) -> pd.DataFrame:
  """
  Duplica una columna en un DataFrame de Pandas.

  Args:
    df: DataFrame a modificar.
    columna_a_duplicar: Nombre de la columna a duplicar.
    columna_nueva: Nombre de la nueva columna.

  Returns:
    DataFrame con la nueva columna duplicada.
  """

  try:
    # Verificamos si la columna a duplicar existe
    if columna_a_duplicar not in df.columns:
      raise KeyError(f"La columna '{columna_a_duplicar}' no existe en el DataFrame.")

    # Verificamos si la columna nueva ya existe
    if columna_nueva in df.columns:
      raise ValueError(f"La columna '{columna_nueva}' ya existe en el DataFrame.")

    # Duplicamos la columna
    df[columna_nueva] = df[columna_a_duplicar].copy()

    logger.info(f"Se duplicó la columna '{columna_a_duplicar}' como '{columna_nueva}'.")
    logger.success("La tarea se completó correctamente.")

    return df

  except KeyError as e:
    logger.critical(f"Error al duplicar la columna '{columna_a_duplicar}': {e}")
    raise KeyError(
        f"La columna '{columna_a_duplicar}' no existe en el DataFrame."
    ) from e

  except ValueError as e:
    logger.critical(f"Error al duplicar la columna '{columna_a_duplicar}': {e}")
    raise ValueError(
        f"La columna '{columna_nueva}' ya existe en el DataFrame."
    ) from e

  except TypeError as e:
    logger.critical(f"Error al duplicar la columna '{columna_a_duplicar}': {e}")
    raise TypeError(
        f"El tipo de datos de la columna '{columna_a_duplicar}' no es compatible con la operación de copia."
    ) from e



@Registro_tiempo
def Duplicar_muliples_columnas(df: pd.DataFrame, columnas_a_duplicar: dict):
    """
    Duplica columnas en un DataFrame de Pandas.

    Args:
        df: DataFrame a modificar.
        columnas_a_duplicar: Diccionario donde las llaves son las columnas a duplicar
                             y los valores son los nombres de las nuevas columnas.

    Returns:
        DataFrame con las nuevas columnas duplicadas.
    """
    try:
        # Verificar si df es un DataFrame
        if not isinstance(df, pd.DataFrame):
            raise ValueError("El argumento 'df' no es un DataFrame.")

        resultado = verificar_existencia_columnas(df=df, columnas=columnas_a_duplicar.keys())

        if resultado == True:
            for columna_origen, columna_nueva in columnas_a_duplicar.items():
                # Duplica la columna
                df[columna_nueva] = df[columna_origen].copy()

                logger.info(f"Se duplicó la columna {columna_origen} como {columna_nueva}.")

            logger.success("La tarea se completó correctamente.")
            return df
        
    except ValueError as ve:
        logger.critical(f"Error: {ve}")
    except Exception as e:
        logger.critical(f"Error al duplicar las columnas: {e}")
        raise e


@Registro_tiempo
def Agregar_columna_constante(
    dataframe: pd.DataFrame, nombre_col_cols: str | list[str], valor_constante: Any
) -> Union[pd.DataFrame, None]:
    """
    Añade una nueva columna con un valor constante a un DataFrame.

    Args:
        dataframe (pd.DataFrame): DataFrame al que se añadirá la nueva columna.
        nombre_col_cols (str | list[str]): Nombre de la nueva columna, o lista con los nombres de las nuevas columnas.
        valor_constante (Any): Valor constante que se asignará a todas las filas de la columna.

    Returns:
        Union[pd.DataFrame, None]: DataFrame con la nueva columna añadida o None si ocurre un error.
    """
    try:
        # Verificar si dataframe es un DataFrame de pandas
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError("El argumento 'dataframe' debe ser un DataFrame de pandas.")

        # Crear una copia del dataframe.
        df = dataframe.copy()
        # Añadir la nueva columna con el valor constante
        df[nombre_col_cols] = valor_constante

        # Registrar el evento
        logger.info(
            f"Se añadieron la columnas '{nombre_col_cols}' con el valor constante '{valor_constante}' al DataFrame."
        )

        return df

    except Exception as e:
        logger.critical(
            f"Error inesperado al añadir columna con valor constante: {str(e)}"
        )
        return None


def Agregar_columnas_constantes(
    dataframe: pd.DataFrame, columnas_valores: dict[str, Any]
) -> Union[pd.DataFrame, None]:
    """
    Añade nuevas columnas con valores constantes a un DataFrame.

    Args:
        dataframe (pd.DataFrame): DataFrame al que se añadirán las nuevas columnas.
        columnas_valores (Dict[str, Any]): Diccionario que asocia nombres de columnas con valores constantes.

    Returns:
        Union[pd.DataFrame, None]: DataFrame con las nuevas columnas añadidas o None si ocurre un error.
    """
    try:
        # Verificar si dataframe es un DataFrame de pandas
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError("El argumento 'dataframe' debe ser un DataFrame de pandas.")

        # Crear una copia del dataframe.
        df = dataframe.copy()

        # Añadir las nuevas columnas con los valores constantes
        for nombre_columna, valor_constante in columnas_valores.items():
            df[nombre_columna] = valor_constante

        # Registrar el evento
        logger.info(
            f"Se añadieron las columnas '{list(columnas_valores.keys())}' con los valores constantes '{list(columnas_valores.values())}' al DataFrame."
        )

        return df

    except Exception as e:
        logger.critical(
            f"Error inesperado al añadir columnas con valores constantes: {str(e)}"
        )
        return None

    except Exception as e:
        logger.critical(
            f"Error inesperado al añadir columnas con valores constantes: {str(e)}"
        )
        return None


@Registro_tiempo
def modificaciones_elegidas_dict_df(
    diccionario: dict, clave: str, columnas: list, operacion: str
) -> dict:
    """
    Modifica un DataFrame en un diccionario realizando una operación en las columnas especificadas.

    Args:
    - diccionario (dict): El diccionario que contiene los DataFrames.
    - clave (str): La clave del DataFrame dentro del diccionario que se modificará.
    - columnas (list): Una lista de las columnas en las que se realizará la operación.
    - operacion (str): La operación a realizar ('astype', 'fillna', 'drop').

    Returns:
    - diccionario modificado (dict)
    """
    # Crear una copia independiente del DataFrame
    df_for_copy = diccionario[clave]
    df_copy = df_for_copy.copy()

    try:
        # Realizar la operación especificada en las columnas
        logger.info(f"Inicio de la operacion {operacion}")
        if operacion == "astype":
            df_copy[columnas] = df_copy[columnas].astype(float)
        elif operacion == "fillna":
            df_copy[columnas] = df_copy[columnas].fillna(0)
        # Agregar más operaciones según sea necesario

        # Asignar la copia de nuevo al diccionario con la misma clave
        diccionario[clave] = df_copy
        logger.success(f"operacion {operacion} completada con exito")

    except Exception as e:
        logger.critical(f"Operacion {operacion} fallida en el dataframe {df_copy}")
        raise e
    return diccionario


def modificaciones_elegidas_dataframe(df=pd.DataFrame, columnas=list, operacion=str):
    """
    Modifica un DataFrame realizando una operación en las columnas especificadas.

    Args:
        df: El dataframe a modificar.
        columnas: Una lista de las columnas en las que se realizará la operación.
        operacion: La operación a realizar dentro del conjunto ('astype', 'fillna', 'drop').

    Returns:
        El dataframe modificado.
    """

    # Realizar la operación especificada en las columnas
    if operacion == "astype":
        df[columnas] = df[columnas].astype(float)
    elif operacion == "fillna":
        df[columnas] = df[columnas].fillna(0)
    # Agregar más operaciones según sea necesario

    return df


def filtrar_indices_por_longitud(df: pd.DataFrame, columna: str, n: int) -> pd.Index:
    """
    Filtra los índices de las filas de un DataFrame donde la columna `columna` tiene un tamaño menor a `n`.

    Args:
        df: El DataFrame a filtrar.
        columna: El nombre de la columna a filtrar.
        n: El tamaño mínimo de la cadena en la columna `columna`.

    Returns:
        Los índices de las filas filtradas.

    """

    logger.info("Filtrando índices de las filas de un DataFrame")
    try:
        indices_a_filtrar = df[df[columna].astype(str).str.len() != n].index
    except Exception as e:
        logger.critical(e)
        raise e

    logger.info("Filtrado completado")
    return indices_a_filtrar


@Registro_tiempo
def modificar_valores_filas(
    df: pd.DataFrame, indices: pd.Index, columna: str, columna_valores: str
) -> pd.DataFrame:
    """
    Reemplaza los valores de las filas filtradas de un DataFrame con los valores de la columna `columna_valores`.

    Args:
        df: El DataFrame a modificar.
        indices: Los índices de las filas a modificar.
        columna: El nombre de la columna a modificar.
        columna_valores: El nombre de la columna con los valores a reemplazar.

    Returns:
        El DataFrame modificado.

    """

    logger.info("Reemplazando valores de las filas filtradas de un DataFrame")
    try:
        for i in indices:
            df.loc[i, columna] = df.loc[i, columna_valores]
    except Exception as e:
        logger.critical(e)
        raise e

    logger.info("Reemplazo completado")
    return df

def modificar_valores_filas2(
    df: pd.DataFrame, indices: pd.Index, columna: str, columna_valores: str
) -> pd.DataFrame:
    """
    Reemplaza los valores de las filas filtradas de un DataFrame con los valores de la columna `columna_valores`.

    Args:
        df: El DataFrame a modificar.
        indices: Los índices de las filas a modificar.
        columna: El nombre de la columna a modificar.
        columna_valores: El nombre de la columna con los valores a reemplazar.

    Returns:
        El DataFrame modificado.

    """
    logger.info("Iniciando el reemplazo de valores en el DataFrame.")
    try:
        # Utiliza `.loc` para una operación de reemplazo vectorizado más eficiente
        df.loc[indices, columna] = df.loc[indices, columna_valores]
    except Exception as e:
        logger.critical("Ocurrió un error al reemplazar valores en el DataFrame", exc_info=True)
        raise e
    else:
        logger.info("El reemplazo de valores se completó exitosamente.")

    return df

# Funcion alternativa a modificar valores filas.
@Registro_tiempo
def reemplazar_indices(
    df: pd.DataFrame, columna_a_reemplazar: str, columna_con_valores: str, indices: list
) -> pd.DataFrame:
    """
    Reemplaza unos índices de una columna por el valor en esos mismos índices de otra columna.

    Args:
        df: DataFrame a modificar.
        columna_a_reemplazar: Columna a reemplazar.
        columna_con_valores: Columna con los valores a reemplazar.
        indices: Índices a reemplazar.

    Returns:
        DataFrame con los índices reemplazados.
    """

    # Obtenemos los valores a reemplazar
    valores_a_reemplazar = df[columna_con_valores].iloc[indices]

    # Reemplazamos los índices
    df.loc[indices, columna_a_reemplazar] = valores_a_reemplazar

    return df

def rellenar_columnas_nulas(
    df: pd.DataFrame | pd.Series,
    columna: str | List[str],
    valor: int | str ,
) -> pd.DataFrame:
    """
    Rellena las columnas nulas de un DataFrame con un valor especificado y reasigna los valores de la columna.

    Args:
        df (pd.DataFrame): DataFrame a rellenar.
        columna ( (str): Nombre de la columna a rellenar ,  list[str] : lista con los nombres de las columnas a rellenar)
        valor (int, str): Valor a utilizar para rellenar las celdas nulas.

    Returns:
        pd.DataFrame: DataFrame con las columnas nulas rellenadas y reasignadas.
    """
    try:
        # Comprueba que la columna/columnas existen en el DataFrame
        if isinstance(columna, list):
            for cada_columna in columna:
                if cada_columna not in df.columns:
                    raise KeyError(
                        f"Columna '{cada_columna}' no existe en el DataFrame."
                    )

            # Crea una copia de las columnas y llena los nulos
            df.loc[:, columna] = df.loc[:, columna].fillna(valor)

        elif columna not in df.columns:
            raise KeyError(f"Columna '{columna}' no existe en el DataFrame.")

        logger.success(f"Remplazo de datos nulos exitoso para las columnas: {columna}")
        return df
    except Exception as e:
        # Manejar cualquier excepción generada durante el proceso
        logger.critical(f"Error: {e} No se realizaron modificaciones al dataframe.")
        return df  


@Registro_tiempo
def pd_left_merge(
    base_left: pd.DataFrame, base_right: pd.DataFrame, key: str
) -> pd.DataFrame:
    """Función que retorna el left join de dos dataframe de pandas.

    Args:
        base_left (pd.DataFrame): Dataframe que será la base del join.
        base_right (pd.DataFrame): Dataframe del cuál se extraerá la información complementaria.
        key (str): Llave mediante la cual se va a realizar el merge o join.

    Returns:
        pd.DataFrame: Dataframe con el merge de las dos fuentes de datos.
    """

    # Validar que base_left y base_right sean DataFrames de pandas
    if not isinstance(base_left, (pd.DataFrame, pd.Series)):
        raise ValueError("El argumento base_left no es un DataFrame de pandas")
    if not isinstance(base_right, (pd.DataFrame, pd.Series)):
        raise ValueError("El argumento base_right no es un DataFrame de pandas")

    base = None

    try:
        base = pd.merge(left=base_left, right=base_right, how="left", on=key)
        logger.success("Proceso de merge satisfactorio")
    except pd.errors.MergeError as e:
        logger.critical(f"Proceso de merge fallido: {e}")
        raise e

    return base


@Registro_tiempo
def pd_left_merge_two_keys(
    base_left: pd.DataFrame,
    base_right: pd.DataFrame,
    left_key: str,
    right_key: str,
) -> pd.DataFrame:
    """Función que retorna el left join de dos dataframe de pandas.

    Args:
        base_left (pd.DataFrame): Dataframe que será la base del join.
        base_right (pd.DataFrame): Dataframe del cuál se extraerá la información complementaria.
        key (str): Llave mediante la cual se va a realizar el merge o join.

    Returns:
        pd.DataFrame: Dataframe con el merge de las dos fuentes de datos.
    """

    # Validar que base_left y base_right sean DataFrames de pandas
    if not isinstance(base_left, (pd.DataFrame, pd.Series)):
        raise ValueError("El argumento base_left no es un DataFrame de pandas")
    if not isinstance(base_right, (pd.DataFrame, pd.Series)):
        raise ValueError("El argumento base_right no es un DataFrame de pandas")

    base = None

    try:
        base = pd.merge(
            left=base_left,
            right=base_right,
            how="left",
            left_on=left_key,
            right_on=right_key,
        )
        logger.success("Proceso de merge satisfactorio")
    except pd.errors.MergeError as e:
        logger.critical(f"Proceso de merge fallido: {e}")
        raise e

    return base


@Registro_tiempo
def merge_inner_dataframes(
    df1: pd.DataFrame, df2: pd.DataFrame, on_column: str
) -> pd.DataFrame:
    """
    Realiza un merge inner de dos DataFrames en pandas basándose en una columna común.

    Parámetros:
    df1 (pd.DataFrame): Primer DataFrame.
    df2 (pd.DataFrame): Segundo DataFrame.
    on_column (str): Nombre de la columna sobre la cual realizar el merge inner.

    Retorna:
    pd.DataFrame: DataFrame resultante del merge inner.
    """
    # Verificar si df1 y df2 son instancias de pd.DataFrame
    if not isinstance(df1, pd.DataFrame) or not isinstance(df2, pd.DataFrame):
        raise TypeError(
            "Los argumentos df1 y df2 deben ser instancias de pd.DataFrame."
        )

    try:
        result = pd.merge(df1, df2, on=on_column, how="inner")
        logger.info(
            f"Merge inner completado correctamente en la columna '{on_column}'."
        )
        return result
    except Exception as e:
        logger.critical(f"Error al realizar el merge inner: {str(e)}")
        return None


@Registro_tiempo
def Lista_unidecode(list_columns: list) -> list:
    """Función que toma una lista con cadenas, y elimina los acentos en cada una.
    Args:
        list_columns: Lista que contiene las cadenas de caracteres a trasformar.
    return:
          = lista de cadenas trasformadas.
    """
    try:
        list_without_accents = [unidecode(i) for i in list_columns]
        logger.success("Proceso de remoción de acentos satisfactorio: ")
    except Exception:
        logger.error("Proceso de remoción de acentos fallido ")
        raise Exception

    return list_without_accents


@Registro_tiempo
def Eliminar_acentos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina las tildes y caracteres acentuados de todas las columnas de un DataFrame.

    Args:
        df (pd.DataFrame): El DataFrame en el que se eliminarán los acentos.

    Returns:
        pd.DataFrame: El DataFrame modificado con los acentos eliminados.
    """
    try:
        logger.info("Iniciando proceso de eliminación de acentos...")
        for columna in df.columns:
            # Verifica si la columna es de tipo objeto (texto)
            if df[columna].dtype == "object":
                # Aplica unidecode a cada valor de la columna y asigna los resultados de nuevo a la columna
                df.loc[:, columna] = [
                    unidecode(x) if pd.notna(x) else x for x in df[columna]
                ]
        logger.success("Proceso de eliminación de acentos completado con éxito.")
        return df
    except Exception as e:
        logger.critical(f"Error en la función eliminar_acentos: {str(e)}")
        return df


@Registro_tiempo
def Unidecode_solo_cols_df(df):
    """
    Convierte a unidecode los nombres de las columnas en un DataFrame.

    Args:
        df (pandas.DataFrame): El DataFrame que se debe modificar.

    Returns:
        pandas.DataFrame: DataFrame modificado en unidecode en nombres de columnas.
    """
    try:
        # Quitar tildes de los nombres de las columnas
        df.columns = [unidecode(col) for col in df.columns]
        logger.success("Unidecode a las columnas del dataframe exitoso.")
        return df

    except Exception as e:
        logger.critical(f"Error al eliminar acentos: {str(e)}")
        raise e


@Registro_tiempo
def Eliminar_primeros_n_caracteres(
    df: pd.DataFrame, columna: str, n: int
) -> pd.DataFrame:
    """
    Elimina los primeros n caracteres de cada fila en la columna especificada del DataFrame.

    Parámetros:
    df (pd.DataFrame): DataFrame a modificar.
    columna (str): Nombre de la columna en la que se realizará la operación.
    n (int): Número de caracteres a eliminar de cada fila en la columna.

    Retorna:
    pd.DataFrame: DataFrame con la columna modificada.
    """
    try:
        # Verificar si df es un DataFrame de pandas y columna es una columna válida en df
        if not isinstance(df, pd.DataFrame):
            raise TypeError("El argumento 'df' debe ser un DataFrame de pandas.")
        if columna not in df.columns:
            raise ValueError(f"La columna '{columna}' no existe en el DataFrame.")

        # Modificar la columna en el DataFrame
        df[columna] = df[columna].apply(
            lambda x: x[n:] if isinstance(x, str) and len(x) > n else x
        )

        # Registrar el proceso
        logger.info(
            f"Se eliminaron los primeros {n} caracteres de la columna '{columna}'."
        )

        return df

    except Exception as e:
        # Registrar el error y retornar None en caso de fallo
        logger.critical(
            f"Error al eliminar los primeros {n} caracteres de la columna '{columna}': {str(e)}"
        )
        return None


def conservar_primeros_n_caracteres(df: pd.DataFrame, columna: str, n: int):
    """
    Conserva los primeros n caracteres de cada valor en la serie de un DataFrame.

    Args:
        df: Un DataFrame.
        columna: El nombre de la columna de la serie a conservar.
        n: El número de caracteres a conservar.

    Returns:
        Un DataFrame con los primeros n caracteres de cada valor en la serie conservados.
    """
    # Capturamos excepciones
    try:
        # Obtenemos el tipo de la serie
        tipo_serie = type(df[columna])

        # Aplicamos la función para conservar los primeros n caracteres
        df[columna] = df[columna].str.slice(0, n)

        # Registramos un mensaje de información
        logger.info(
            "Se han conservado los primeros {} caracteres de la serie {} del DataFrame.".format(
                n, columna
            )
        )

        # Devolvemos el DataFrame modificado
        return df

    except Exception as e:
        # Registramos un mensaje de error
        logger.exception(e)


@Registro_tiempo
def Eliminar_espacios_finales(
    df: pd.DataFrame, columna: str
) -> Union[pd.DataFrame, None]:
    """
    Elimina los espacios en blanco al final de los datos en una columna específica del DataFrame.

    Parámetros:
    df (pd.DataFrame): DataFrame a modificar.
    columna (str): Nombre de la columna en la que se realizará la operación.

    Retorna:
    pd.DataFrame: DataFrame con la columna modificada.
    """
    try:
        # Verificar si df es un DataFrame de pandas y columna es una columna válida en df
        if not isinstance(df, pd.DataFrame):
            raise TypeError("El argumento 'df' debe ser un DataFrame de pandas.")
        if columna not in df.columns:
            raise ValueError(f"La columna '{columna}' no existe en el DataFrame.")

        # Aplicar la operación para eliminar espacios en blanco al final
        df[columna] = df[columna].str.rstrip()

        # Registrar el proceso
        logger.info(
            f"Se eliminaron los espacios en blanco al final de la columna '{columna}'."
        )

        return df

    except Exception as e:
        # Registrar el error y retornar None en caso de fallo
        logger.critical(
            f"Error al eliminar espacios en blanco al final de la columna '{columna}': {str(e)}"
        )
        return None


@Registro_tiempo
def Cambiar_tipo_dato_multiples_columnas(
    base: pd.DataFrame, list_columns: list, type_data: type
) -> pd.DataFrame:
    """
    Función que toma un DataFrame, una lista de sus columnas para hacer un cambio en el tipo de dato de las mismas.

    Args:
        base (pd.DataFrame): DataFrame que es la base del cambio.
        list_columns (list): Columnas a modificar su tipo de dato.
        type_data (type): Tipo de dato al que se cambiarán las columnas (ejemplo: str, int, float).

    Returns:
        pd.DataFrame: Copia del DataFrame con los cambios.
    """
    try:
        # Verificar que el DataFrame tenga las columnas especificadas
        for columna in list_columns:
            if columna not in base.columns:
                raise KeyError(f"La columna '{columna}' no existe en el DataFrame.")

        # Cambiar el tipo de dato de las columnas
        base_copy = (
            base.copy()
        )  # Crear una copia para evitar problemas de SettingWithCopyWarning
        base_copy[list_columns] = base_copy[list_columns].astype(type_data)

        return base_copy

    except Exception as e:
        logger.critical(f"Error en Cambiar_tipo_dato_multiples_columnas: {e}")


def cols_a_num_seleccionado(df):
        """
        Intenta convertir todas las columnas de un DataFrame a tipo numérico.
        Si una columna no puede ser convertida completamente a numérico, se deja sin cambios.

        Args:
        df (pd.DataFrame): DataFrame que contiene las columnas a convertir.

        Returns:
        pd.DataFrame: DataFrame con las columnas convertidas a tipo numérico cuando es posible.
        """
        for col in df.columns:
            try:
                if not isinstance(df, pd.DataFrame):
                    raise TypeError(
                        "El argumento 'df' debe ser un DataFrame de pandas."
                    )

                df[col] = df[col].astype(float)
            except ValueError:
                pass  # Si no puede convertir la columna, pasa a la siguiente
        return df

def seleccionar_columnas(
    dataframe: pd.DataFrame, cols_elegidas: List[str]
) -> Union[pd.DataFrame, None]:
    """
    Filtra y retorna las columnas especificadas del DataFrame.

    Parámetros:
    dataframe (pd.DataFrame): DataFrame del cual se filtrarán las columnas.
    cols_elegidas (list): Lista de nombres de las columnas a incluir en el DataFrame filtrado.

    Retorna:
    pd.DataFrame: DataFrame con las columnas filtradas.
    """
    try:
        # Verificar si dataframe es un DataFrame de pandas
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError("El argumento 'dataframe' debe ser un DataFrame de pandas.")

        # Filtrar las columnas especificadas
        df_filtrado = dataframe[cols_elegidas]

        # Registrar el proceso
        logger.info(f"Columnas filtradas: {', '.join(cols_elegidas)}")

        return df_filtrado

    except KeyError as ke:
        logger.critical(
            f"Error: Columnas especificadas no encontradas en el DataFrame: {str(ke)}"
        )
        return None
    except Exception as e:
        logger.critical(f"Error inesperado al filtrar columnas: {str(e)}")
        return None


def Agregar_filas_no_existentes(df1: pd.DataFrame, df2: pd.DataFrame, columna_id: str):
    """
    Agrega a un dataframe1 las filas del dataframe2 donde los id de dataframe2 no estén
    en dataframe1.

    Args:
        df1: El dataframe1 a modificar.
        df2: El dataframe2 a agregar.
        columna_id: La columna que se utiliza para comparar los id.

    Returns:
        El dataframe1 modificado junto con los id adicionales.
    """

    # Verificar que ambos DataFrames tengan las mismas columnas.
    try:
        if not set(df1.columns) == set(df2.columns):
            raise ValueError("Los DataFrames deben tener las mismas columnas.")
    except ValueError as e:
        logger.critical(e)

    # Obtener los id del dataframe1.
    id_df1 = df1[columna_id].tolist()

    # Filtrar las filas del dataframe2 donde los id no estén en el dataframe1.
    df2_filtrado = df2[~df2[columna_id].isin(id_df1)]

    # Agregar las filas filtradas al dataframe1.
    df1 = pd.concat([df1, df2_filtrado], axis=0)

    return df1


def crear_pivot_table(df, lista_columnas, columna_agrupacion, columna_valor, funcion_agregacion="sum", fill_value = 0):
    """
    Crea una tabla dinámica (pivot table) a partir de un DataFrame dado y resetea los índices.

    Parámetros:
    -----------
    df : pandas.DataFrame
        El df que contiene los datos a pivotar.
    lista_columnas : list
        Lista de nombres de columnas que serán utilizadas como índices (filas) en la tabla dinámica.
    columna_agrupacion : str
        El nombre de la columna que será usada como las columnas en la tabla dinámica.
    columna_valor : str
        El nombre de la columna que será usada para los valores en la tabla dinámica.
    funcion_agregacion : str, opcional (por defecto "sum")
        La función de agregación a aplicar en los valores (por ejemplo: "sum", "mean", etc.).

    Retorna:
    --------
    pandas.DataFrame
        La tabla dinámica generada con los índices reseteados.
    """
    # Crear la tabla dinámica
    pivot_table = pd.pivot_table(
        df,
        index=lista_columnas,  # Filas
        columns=columna_agrupacion,  # Columnas
        values=columna_valor,  # Valores
        aggfunc=funcion_agregacion,  # Función de agregación
        fill_value = 0
    )

    # Resetear los índices
    pivot_table_reseteada = pivot_table.reset_index()

    return pivot_table_reseteada


def concatenar_columnas(
    dataframe: pd.DataFrame, cols_elegidas: List[str], nueva_columna: str
) -> Union[pd.DataFrame, None]:
    """
    Concatena las columnas especificadas y agrega el resultado como una nueva columna al DataFrame.

    Parámetros:
    dataframe (pd.DataFrame): DataFrame del cual se concatenarán las columnas.
    cols_elegidas (list): Lista de nombres de las columnas a concatenar.
    nueva_columna (str): Nombre de la nueva columna que contendrá el resultado de la concatenación.

    Retorna:
    pd.DataFrame: DataFrame con la nueva columna agregada.
    """
    try:
        # Verificar si dataframe es un DataFrame de pandas
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError("El argumento 'dataframe' debe ser un DataFrame de pandas.")

        # Verificar si las columnas especificadas existen en el DataFrame
        for col in cols_elegidas:
            if col not in dataframe.columns:
                raise KeyError(f"La columna '{col}' no existe en el DataFrame.")

        # Concatenar las columnas especificadas y agregar el resultado como una nueva columna
        dataframe[nueva_columna] = (
            dataframe[cols_elegidas].fillna("").agg("".join, axis=1)
        )

        # Registrar el proceso
        logger.info(
            f"Columnas '{', '.join(cols_elegidas)}' concatenadas y almacenadas en '{nueva_columna}'."
        )

        return dataframe

    except Exception as e:
        logger.critical(f"Error inesperado al concatenar columnas: {str(e)}")
        return None


def Filtrar_por_valores_excluidos(
    df: pd.DataFrame, columna: str, valores_excluir: List[Union[str, int]]
) -> pd.DataFrame:
    """
    Filtra el DataFrame excluyendo las filas que contienen valores especificados en una columna.

    Args:
        columna (pd.Series): Columna del DataFrame a filtrar.
        valores_excluir (List[Union[str, int]]): Lista de valores a excluir en el filtro.

    Returns:
        pd.DataFrame: DataFrame filtrado excluyendo las filas con valores especificados.
    """
    try:
        if isinstance(valores_excluir, str):
            valores_excluir = [valores_excluir]
        # Filtrar el DataFrame excluyendo las filas con valores especificados
        df_filtrado = df[~df[columna].isin(valores_excluir)]

        return df_filtrado

    except Exception as e:
        logger.critical(f"Error inesperado al filtrar por valores excluidos: {str(e)}")
        return None


def Filtrar_indices_por_longitud(
    dataframe: pd.DataFrame, columna: str, longitud_deseada: int
) -> Union[List[int], None]:
    """
    Filtra los índices del DataFrame donde los datos de la columna especificada tienen la longitud deseada.

    Args:
        dataframe (pd.DataFrame): DataFrame a ser filtrado.
        columna (str): Nombre de la columna en la que se verificará la longitud.
        longitud_deseada (int): Longitud deseada para los datos en la columna.

    Returns:
        Union[List[int], None]: Lista de índices que cumplen con la condición o None si ocurre un error.
    """
    try:
        # Verificar si dataframe es un DataFrame de pandas
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError("El argumento 'dataframe' debe ser un DataFrame de pandas.")

        # Filtrar índices donde los datos de la columna tienen la longitud deseada
        indices_filtrados = dataframe[
            dataframe[columna].str.len() == longitud_deseada
        ].index.tolist()

        # Registrar el proceso
        logger.info(
            f"Índices filtrados por longitud {longitud_deseada} en la columna '{columna}': {indices_filtrados}"
        )

        return indices_filtrados

    except Exception as e:
        logger.critical(f"Error inesperado al filtrar índices por longitud: {str(e)}")
        return None


@Registro_tiempo
def filtrar_por_valores(
    df: pd.DataFrame, columna: str, valores_filtrar: List[Union[str, int]]
) -> pd.DataFrame:
    """
    Filtra el DataFrame basándose en los valores de una columna específica.

    Args:
        columna (pd.Series): Columna del DataFrame a filtrar.
        valores_filtrar (List[Union[str, int]]): Lista de valores a utilizar para filtrar la columna.

    Returns:
        pd.DataFrame: DataFrame filtrado basándose en los valores especificados.
    """
    try:
        if isinstance(valores_filtrar, str):
            valores_filtrar = [valores_filtrar]

        # Filtrar el DataFrame basándose en los valores de la columna
        df_filtrado = df[df[columna].isin(valores_filtrar)]

        return df_filtrado

    except Exception as e:
        logger.critical(f"Error inesperado al filtrar por valores: {str(e)}")
        return None


@Registro_tiempo
def Formatear_primera_letra_mayuscula(
    dataframe: pd.DataFrame, columna: str
) -> Union[pd.DataFrame, None]:
    """
    Pone la primera letra en mayúscula de las columnas especificadas en un DataFrame.

    Args:
        dataframe (pd.DataFrame): DataFrame a ser formateado.
        columnas (List[str]): Lista de nombres de columnas a las que se les aplicará el formato.

    Returns:
        Union[pd.DataFrame, None]: DataFrame con las columnas especificadas formateadas o None si ocurre un error.
    """
    try:
        # Verificar si dataframe es un DataFrame de pandas
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError("El argumento 'dataframe' debe ser un DataFrame de pandas.")

        # Aplicar la primera letra en mayúscula a las columnas especificadas
        """Como precuación: Transformamos el tipo de dato a str. -> astype(str)"""

        dataframe.loc[:, columna] = (
            dataframe[columna].astype(str).apply(lambda x: x.title())
        )

        logger.info(f"Primera letra en mayúscula aplicada a las columnas: {columna}")

        return dataframe

    except Exception as e:
        logger.critical(f"Error inesperado al formatear las columnas: {str(e)}")
        return None


# (Eliminación de filas tomando 100% de las columnas.)
@Registro_tiempo
def Eliminar_filas_duplicadas(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina las filas duplicadas en un DataFrame y devuelve el DataFrame resultante.

    Args:
        dataframe (pd.DataFrame): DataFrame en el que se eliminarán las filas duplicadas.

    Returns:
        pd.DataFrame: DataFrame sin filas duplicadas.
    """
    try:
        # Verificar si dataframe es un DataFrame de pandas
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError("El argumento 'dataframe' debe ser un DataFrame de pandas.")

        # Eliminar filas duplicadas
        dataframe_sin_duplicados = dataframe.drop_duplicates()

        return dataframe_sin_duplicados

    except Exception as e:
        print(f"Error al eliminar filas duplicadas: {str(e)}")
        return None


def Eliminar_filas_duplicadas_por_columnas(
    dataframe: pd.DataFrame, columnas: Union[List[str], str]
) -> Union[pd.DataFrame, None]:
    """
    Elimina las filas duplicadas basándose en las columnas especificadas y devuelve el DataFrame resultante.

    Args:
        dataframe (pd.DataFrame): DataFrame en el que se eliminarán las filas duplicadas.
        columnas (List[str]): Lista de nombres de columnas para identificar las filas duplicadas.

    Returns:
        Union[pd.DataFrame, None]: DataFrame sin filas duplicadas o None si ocurre un error.
    """
    try:
        # Verificar si dataframe es un DataFrame de pandas
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError("El argumento 'dataframe' debe ser un DataFrame de pandas.")

        if isinstance(columnas, str):
            columnas = [columnas]

        # Eliminar filas duplicadas basándose en las columnas especificadas
        dataframe_sin_duplicados = dataframe.drop_duplicates(subset=columnas)

        # Registrar el proceso
        logger.info(
            f"Filas duplicadas eliminadas basándose en las columnas: {columnas}"
        )

        return dataframe_sin_duplicados

    except Exception as e:
        logger.critical(f"Error inesperado al eliminar filas duplicadas: {str(e)}")
        return None


def Group_by_and_sum_cols(df=pd.DataFrame, group_col=list, sum_col=list):
    """
    Agrupa un DataFrame por una columna y calcula la suma de otra columna.

    Args:
        df (pandas.DataFrame): El DataFrame que se va a agrupar y sumar.
        group_col (list or str): El nombre de la columna o lista de nombres de columnas por la cual se va a agrupar.
        sum_col (list or str): El nombre de la columna o lista de nombres de columnas que se va a sumar.

    Returns:
        pandas.DataFrame: El DataFrame con las filas agrupadas y la suma calculada.
    """

    try:
        if isinstance(group_col, str):
            group_col = [group_col]

        if isinstance(sum_col, str):
            sum_col = [sum_col]

        result_df = df.groupby(group_col, as_index=False)[sum_col].sum()

        # Registro de éxito
        logger.info(f"Agrupación y suma realizadas con éxito en las columnas.")

    except Exception as e:
        # Registro de error crítico
        logger.critical(f"Error al realizar la agrupación y suma en las columnas. {e}")
        result_df = None

    return result_df


def obtener_valores_unicos_ordenados(df, columna):
    """
    Obtiene una lista ordenada con los valores únicos de una columna de un DataFrame.

    Args:
      df: El DataFrame que contiene la columna.
      columna: El nombre de la columna.

    Returns:
      Una lista ordenada con los valores únicos de la columna.

    Raises:
      KeyError: Si la columna no existe en el DataFrame.
      TypeError: Si la columna no es de tipo `pd.Series`.
    """

    # Verifica que la columna exista en el DataFrame.

    try:
        columna_df = df[columna]
    except KeyError as e:
        raise e

    # Verifica que la columna sea de tipo `pd.Series`.

    if not isinstance(columna_df, pd.Series):
        raise TypeError("La columna `{}` no es de tipo `pd.Series`.".format(columna))

    # Devuelve una lista ordenada con los valores únicos de la columna.

    return sorted(columna_df.unique())


@Registro_tiempo
def Reemplazar_columna_con_valor_constante(
    df: pd.DataFrame, columna: str, valor_reemplazo: any
):
    """
    Reemplaza todos los valores en la columna especificada de un DataFrame con un valor constante.

    Parámetros:
    - df (pd.DataFrame): El DataFrame que se modificará.
    - columna (str): El nombre de la columna que se va a reemplazar.
    - valor_reemplazo (any): El valor constante para reemplazar todos los valores existentes en la columna especificada.

    Retorna:
    - pd.DataFrame: El DataFrame modificado con los valores de la columna especificada reemplazados.

    Lanza:
    - TypeError: Si 'df' no es un DataFrame de pandas.
    - KeyError: Si la 'columna' especificada no se encuentra en el DataFrame.

    Ejemplo:
    ```python
    import pandas as pd

    # Crear un DataFrame de ejemplo
    datos = {'A': [1, 2, 3], 'B': [4, 5, 6]}
    df = pd.DataFrame(datos)

    # Reemplazar la columna 'A' con el valor constante 10
    df_resultado = reemplazar_columna_con_valor_constante(df, 'A', 10)
    ```

    Registro:
    - Registra un mensaje de éxito después de un reemplazo exitoso.
    - Registra un mensaje crítico con un error si ocurre una excepción durante la operación.

    """
    try:
        # Verificar si la entrada es un DataFrame de pandas
        if not isinstance(df, pd.DataFrame):
            raise TypeError("El argumento 'df' debe ser un DataFrame de pandas.")

        # Verificar si la columna especificada existe en el DataFrame
        if columna not in df.columns:
            raise KeyError(f"Columna '{columna}' no encontrada en el DataFrame.")

        # Realizar el reemplazo
        df.loc[:, columna] = valor_reemplazo

        # Registrar mensaje de éxito
        logger.success(
            f"Columna '{columna}' reemplazada con el valor '{valor_reemplazo}'"
        )

        return df

    except Exception as e:
        # Registrar mensaje crítico si ocurre una excepción
        logger.critical(f"Error durante el reemplazo de la columna. {e}")
        return None


def Seleccionar_bases_por_periodos(
    df: pd.DataFrame, Periodo: list, dict_meses: dict, hay_trim: str, mes_act
) -> dict:
    """
    Selecciona las bases de datos para un período de tiempo dado.

    Args:
        df: El DataFrame que contiene los datos.
        Periodo: La lista de períodos de tiempo.

    Returns:
        La lista `Bases` con las bases de datos seleccionadas.

    Documentación detallada:

    La función `Selccionar_bases_por_periodos()` recibe dos argumentos:

    * `df`: El DataFrame que contiene los datos.
    * `Periodo`: La lista de períodos de tiempo.
    * dict_meses: "Diccionario de referencia para los meses."

    La función crea tres bases de datos:

    * **Acumulado:** La base de datos que contiene todos los datos.
    * **Mes actual:** La base de datos que contiene los datos del mes actual.
    * **Trimestre actual:** La base de datos que contiene los datos del trimestre actual.

    Si la lista `Periodo` tiene más de dos elementos, se crea una base de datos adicional   para el trimestre actual.
    """
    # Definimos variables locales constantes.
    dict_bases = {}
    TRIMESTRE = "Trim"
    ACUMULADO = "Acum"

    while True:
        try:
            # Solicitar al usuario que ingrese la abreviatura del mes actual
            ACTUAL = mes_act
            # Verificar si la entrada del usuario está en la tupla de meses válidos
            if ACTUAL in dict_meses.keys():
                break  # Salir del bucle si la entrada es válida
            else:
                logger.info(
                    "Entrada no válida. Por favor, ingrese una abreviatura de mes válida."
                )

        except Exception as e:
            logger.critical(f"Error: {e}")

    # Tomar la Sub-Lista de meses correspondiente de acuerdo al mes ingresado.

    # Consultamos el mes por la llave ya ingresada.
    num_mes_actual = dict_meses[ACTUAL]

    # Tomamos el subconjunto correspondiente de la lista desde el elemento 0 hasta el mes actual.
    Periodo_filtrado = extraer_sublista_hasta_elemento(
        lista=Periodo, elemento_final=num_mes_actual
    )

    df_acumulado = filtrar_por_valores(
        df=df, columna="Mes", valores_filtrar=Periodo_filtrado
    )
    df_mes_actual = filtrar_por_valores(
        df=df, columna="Mes", valores_filtrar=Periodo_filtrado[-1]
    )

    if (len(Periodo_filtrado) > 2) and (hay_trim == "si"):
        df_trimestre = filtrar_por_valores(
            df=df, columna="Mes", valores_filtrar=Periodo_filtrado[-3:]
        )
        dict_bases = {
            ACUMULADO: df_acumulado,
            ACTUAL: df_mes_actual,
            TRIMESTRE: df_trimestre,
        }
    else:
        dict_bases = {ACUMULADO: df_acumulado, ACTUAL: df_mes_actual}

    return dict_bases


def establecer_periodo(df: pd.DataFrame, dict_meses: dict , hay_trim: str, mes_act: str) -> pd.DataFrame:
    """
    Establece el periodo en un DataFrame específico del proyecto Seguimiento a la Inversión de Comercial Nutresa.

    Parameters:
    - df (pd.DataFrame): El DataFrame de entrada.
    - dict_meses (dict): El diccionario que mapea los números de mes a sus respectivos nombres.

    Returns:
    - pd.DataFrame or None: El DataFrame resultante después de aplicar los procedimientos,
      o None si ocurre un error durante el procesamiento.

    Note:
    Esta función es exclusiva del proyecto Seguimiento a la Inversión de Comercial Nutresa.
    """
    try:
        # Paso 1: Conservar los primeros n caracteres de la columna "Mes"
        base = conservar_primeros_n_caracteres(df=df, columna="Mes", n=3)

        # Paso 2: Copiar el DataFrame resultante del paso 1
        base_copy = base.copy()

        # Paso 3: Mapear valores en la columna "Mes" usando el diccionario proporcionado
        base_copy.loc[:, "Mes"] = base_copy.loc[:, "Mes"].apply(
            lambda x: dict_meses.get(x)
        )

        # Paso 4: Obtener valores únicos ordenados de la columna "Mes"
        meses = obtener_valores_unicos_ordenados(df=base_copy, columna="Mes")

        # Paso 5: Seleccionar bases por periodos
        bases_para_agrup = Seleccionar_bases_por_periodos(
            df=base_copy, Periodo=meses, dict_meses=dict_meses , hay_trim=hay_trim, mes_act=mes_act
        )

        # Paso 6: Reemplazar los valores de la columna "Mes" por valores constantes correspondientes a cada periodo
        bases_para_agrup_list = [
            Reemplazar_columna_con_valor_constante(
                df=value, columna="Mes", valor_reemplazo=key
            )
            for key, value in bases_para_agrup.items()
        ]

        # Paso 7: Concatenar los DataFrames resultantes del paso 6
        base_concat = concatenar_dataframes(df_list=bases_para_agrup_list)

        # Paso 8: Tomar el nombre de la varaible que guardó los resultados.

        return base_concat

    except Exception as e:
        # Capturar y manejar cualquier excepción que ocurra durante el procesamiento
        logger.critical(f"Error al procesar el DataFrame: {e}")
    return None
