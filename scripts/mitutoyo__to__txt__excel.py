import os
import time
import threading
import pandas as pd
import xlwings as xw
import glob
import json
import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

RUTA_TXT = r"C:\Users\ivan_\Desktop\Proyecto\medidas.txt"

def obtener_ultimo_archivo_excel(carpeta):
    archivos = glob.glob(os.path.join(carpeta, "*.xlsx"))
    archivos = [f for f in archivos if not os.path.basename(f).startswith('~$')]
    if not archivos:
        raise FileNotFoundError("No se encontraron archivos .xlsx válidos en la carpeta")
    return max(archivos, key=os.path.getmtime)

carpeta = r'C:\Mitutoyo\USB-ITPAK\Excel'
archivo_origen = obtener_ultimo_archivo_excel(carpeta)
archivo_destino = r'C:\Users\ivan_\Documents\MEDIDAS_GALGA.xlsm'

valores_previos = set()
columna_actual = 3
fila_actual = None
ultima_fila_procesada = 0

def insertar_fila_completa_en_txt(ruta_archivo, fila, ruta_txt):
    try:
        app = xw.apps.active if xw.apps else xw.App(visible=False)
        wb = None
        for book in app.books:
            if book.fullname.lower() == ruta_archivo.lower():
                wb = book
                break
        if wb is None:
            wb = app.books.open(ruta_archivo)
        sht = wb.sheets["MEDIDAS"]

        fecha_hora = sht.cells(fila, 1).value
        codigo_pieza = sht.cells(fila, 2).value
        medidas = [sht.cells(7, col).value for col in range(3, 11)]
        valores = [sht.cells(fila, col).value for col in range(3, 11)]

        documentos = []
        for medida, valor in zip(medidas, valores):
            doc = {
                "fecha_hora": fecha_hora,
                "codigo_pieza": codigo_pieza,
                "medida": medida,
                "valor": valor
            }

            for k, v in doc.items():
                if isinstance(v, datetime.datetime):
                    doc[k] = v.isoformat()

            documentos.append(doc)

        with open(ruta_txt, "a", encoding="utf-8") as f:
            for doc in documentos:
                f.write(json.dumps(doc) + "\n")

        print(f"Guardados {len(documentos)} documentos en {ruta_txt}")
    except Exception as e:
        print(f"Error escribiendo en .txt fila {fila}: {e}")

def insertar_valor_individual(ruta_archivo, valor):
    global fila_actual, columna_actual

    try:
        try:
            app = xw.apps.active
        except Exception:
            app = xw.App(visible=True)

        wb = None
        for book in app.books:
            if book.fullname.lower() == ruta_archivo.lower():
                wb = book
                break
        if wb is None:
            wb = app.books.open(ruta_archivo)

        sht = wb.sheets["MEDIDAS"]
        wb.activate()
        sht.activate()

        if fila_actual is None or columna_actual is None:
            celda = sht.api.Application.ActiveCell
            fila_actual = celda.Row
            columna_actual = 3

        celda_destino = sht.cells(fila_actual, columna_actual)
        celda_destino.value = valor

        print(f"Insertado '{valor}' en celda {celda_destino.address} (Fila {fila_actual}, Columna {columna_actual})")

        if columna_actual >= 10:
            insertar_fila_completa_en_txt(ruta_archivo, fila_actual, RUTA_TXT)
            fila_actual += 1
            columna_actual = 3
        else:
            columna_actual += 1

        wb.save()

    except Exception as e:
        print(f"Error al insertar valor: {e}")

class ExcelChangeHandler(FileSystemEventHandler):
    def on_modified(self, event):
        print(f"Evento detectado: {event.src_path}")
        global ultima_fila_procesada
        if os.path.abspath(event.src_path) == os.path.abspath(archivo_origen):
            print(f"{archivo_origen} ha cambiado, revisando nuevos valores...")
            try:
                df = pd.read_excel(archivo_origen)
                if df.empty or df.shape[1] < 1:
                    print("El archivo está vacío o no tiene columnas.")
                    return

                columna = df.iloc[:, 0].dropna()
                if ultima_fila_procesada >= len(columna):
                    print("No hay filas nuevas desde la última revisión.")
                    return

                nuevas_filas = columna.iloc[ultima_fila_procesada:]
                print(f"Filas nuevas encontradas: {nuevas_filas.tolist()}")

                for valor in nuevas_filas:
                    if isinstance(valor, (int, float)):
                        insertar_valor_individual(archivo_destino, valor)
                    else:
                        print(f"Valor no numérico ignorado: {valor}")

                ultima_fila_procesada += len(nuevas_filas)

            except Exception as e:
                print(f"Error leyendo archivo: {e}")

if __name__ == "__main__":
    try:
        df_inicial = pd.read_excel(archivo_origen)
        if df_inicial.empty or df_inicial.shape[1] < 1:
            print("El archivo inicial está vacío o no tiene columnas.")
        else:
            valores_previos = set(df_inicial.iloc[:, 0].dropna().unique())
    except Exception as e:
        print(f"No se pudo cargar el archivo inicial: {e}")

    event_handler = ExcelChangeHandler()
    observer = Observer()
    carpeta = os.path.dirname(os.path.abspath(archivo_origen))
    observer.schedule(event_handler, path=carpeta, recursive=False)
    observer.start()
    print(f"Monitoreando cambios en {archivo_origen}...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
