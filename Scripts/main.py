# Creación del main.py
# Importamos las librerias necesarias para la configuración.
import sys
import pandas as pd
from loguru import logger
from os import getcwd, chdir, path

# Preguntamos al usuario por el lugar de ejecución.
lugar_de_ejecucion = input(
    "Está ejecutando esta automatización desde Python IDLE ó desde cmd?: (si/no): "
)

if lugar_de_ejecucion == "si":
    ruta_actual = getcwd()
    ruta_padre = path.dirname(ruta_actual)
    chdir(ruta_padre)
else:
    pass

# Configuramos la ruta para traer las funciones.
current_dir = path.dirname(path.abspath(__file__))
parent_dir = path.dirname(current_dir)
new_dir = path.join(parent_dir, "Utils")
new_dir2 = path.join(parent_dir, "Scripts")
sys.path.append(new_dir)
sys.path.append(new_dir2)


# Importamos la funcion especifica para medir el tiempo de ejecución del programa.
# Importamos el modulo de la configuración.
from General_Functions import Registro_tiempo, Exportar_dataframes_mismo_excel


# Agregamos el decorador para medir el tiempo de ejecución del programa.
@Registro_tiempo
def Proceso_Seguimiento_a_la_inversion():
    ## Importamos el modulo logger_funtions para llevar registro de funciones con #logs
    from Clientes_plan_negocio import base_biblia_filtrada

    from Estructura_Ventas import (
        base_driver_estructura_venta_rename,
        base_nom_figura_comer_rename,
    )
    from socios import base_socios_filtrada_total_rename
    from limpieza_maestras import (
        clientes_medibles,
        maestra_cliente_NifMaestra_filtrada,
        driver_cod_jv_ven_filtrada_2,
    )
    from Clientes_plan_negocio import base_biblia_filtrada

    dict_dataframes = {
        "Clientes_Plan_de_Negocios": base_biblia_filtrada,
        "Clientes_Socios": base_socios_filtrada_total_rename,
        "Nombre_figura_comercial": base_nom_figura_comer_rename,
        "Cods_JV_y_RV": driver_cod_jv_ven_filtrada_2,
        "Estructura_de_Ventas": base_driver_estructura_venta_rename,
        "Cliente_Nit_maestra": maestra_cliente_NifMaestra_filtrada,
        "Clientes_medibles": clientes_medibles,
    }

    Exportar_dataframes_mismo_excel(
        dataframes=dict_dataframes, nombre_archivo="Drivers/Drivers_automatizacion.xlsx"
    )

    while True:
        try:
            # Solicitar al usuario que ingrese la abreviatura del mes actual
            respuesta = str(
                input(
                    "¿Está conforme con los resultados de los drivers? (si / no). Responda unicamente 'si' para continuar el proceso, 'no' para detenerlo, las respuestas deben ser ingresadas sin comillas. "
                )
            )
            # Verificar si la entrada del usuario sea válida.
            if respuesta == "si":
                try:
                    import Plantilla1

                    break  # Salir del bucle
                except Exception as e:
                    logger.critical(f"Error al importar Plantilla1: {e}")
                    break  # Salir del bucle en caso de error en la importación
            elif respuesta == "no":
                logger.info("Proceso cancelado")
                break
            else:
                logger.info(
                    "Entrada no válida. Por favor, ingrese una respuesta válida (si / no)"
                )
        except Exception as e:
            logger.critical(f"Error: {e}")
            break  # Salir del bucle en caso de cualquier otra excepción


if __name__ == "__main__":
    Proceso_Seguimiento_a_la_inversion()
