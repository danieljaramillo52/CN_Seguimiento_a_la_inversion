# Creación del main.py
# Importamos las librerias necesarias para la configuración.
import sys
from os import getcwd, chdir, path

# Configuramos la ruta para traer las funciones.
parent_dir = path.dirname(path.dirname(path.abspath(__file__)))

# Agregamos los directorios de Utils (Contiene todas las funciones y Scritps)
sys.path.extend([f"{parent_dir}\\Utils", f"{parent_dir}\\Scripts", f"{parent_dir}\\Scripts_P2"])

from General_Functions import Registro_tiempo

# Agregamos el decorador para medir el tiempo de ejecución del programa.
@Registro_tiempo
def Proceso_Seguimiento_a_la_inversion():
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
    
    # Importamos el modulo logger_funtions para llevar registro de funciones con logs
    import logger_functions
    import Plantilla_2_consolidada


if __name__ == "__main__":
    Proceso_Seguimiento_a_la_inversion()
