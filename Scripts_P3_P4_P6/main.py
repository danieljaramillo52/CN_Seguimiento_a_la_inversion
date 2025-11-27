# Creación del main.py
# Importamos las librerias necesarias para la configuración.
import sys
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
parent_dir = path.dirname(path.dirname(path.abspath(__file__)))
sys.path.extend(
    [f"{parent_dir}\\Utils", f"{parent_dir}\\Scripts", f"{parent_dir}\\Scripts_P2", f"{parent_dir}\\Scripts_P3_P4_P6"]
)
# Importamos la funcion especifica para medir el tiempo de ejecución del programa.
# Importamos el modulo de la configuración.
from General_Functions import Registro_tiempo


# Agregamos el decorador para medir el tiempo de ejecución del programa.
@Registro_tiempo
def Proceso_Seguimiento_a_la_inversion():
    # Importamos el modulo logger_funtions para llevar registro de funciones con logs
    import logger_functions
    import cols_adicionales_plantilla_3_4_6




if __name__ == "__main__":
    Proceso_Seguimiento_a_la_inversion()
