import os
import glob
import pandas as pd
from unidecode import unidecode

# 1. Definir las rutas de las subcarpetas
carpetas = [
    r'C:\ruta\A',  # Reemplaza con la ruta real
    r'C:\ruta\B'   # Reemplaza con la ruta real
]

# 2. Listar todos los archivos de Excel en las subcarpetas, excluyendo archivos que contengan "servigral"
archivos_excel = []
for carpeta in carpetas:
    archivos_en_carpeta = glob.glob(os.path.join(carpeta, '**', '*.xlsx'), recursive=True)
    # Filtrar archivos que no contengan "servigral" en el nombre
    archivos_en_carpeta = [archivo for archivo in archivos_en_carpeta if "servigral" not in archivo.lower()]
    archivos_excel.extend(archivos_en_carpeta)

# 3. Definir las columnas relevantes
columnas_relevantes = [
    'Nbre.Complet.Empresa',
    'FACTURA',
    'FECHA',
    'PROVEEDOR',
    'SUB ZONA',
    'ZONA',
    'LINEA DE NEGOCIO',
    'TIPO ( PRODUCTO)',
    'No. de Parte',
    'Descripc. de Parte',
    'Familia',
    'Descripc.de Familia',
    'Cantidad',
    'Ingreso por Venta',
    'Año',
    'Mes',
    'Bodega Despacho'
]

# 4. Crear un DataFrame vacío para almacenar los datos combinados
datos_ventas = pd.DataFrame()

# 5. Leer y consolidar los archivos de Excel
for archivo in archivos_excel:
    try:
        # Leer la hoja 'DataSheet' del archivo Excel
        df = pd.read_excel(
            archivo,
            sheet_name='DataSheet',
            usecols=columnas_relevantes,
            dtype={
                'No. de Parte': str,
                'Bodega Despacho': str
            }
        )

        # Convertir 'No. de Parte' y 'Bodega Despacho' a texto
        df['No. de Parte'] = df['No. de Parte'].astype(str)
        df['Bodega Despacho'] = df['Bodega Despacho'].astype(str)

        # Eliminar cualquier decimal o ".0" en 'Bodega Despacho'
        df['Bodega Despacho'] = df['Bodega Despacho'].str.replace(r'\.0$', '', regex=True)

        # Excluir registros de 'IFRS Servigral'
        df = df[df['Nbre.Complet.Empresa'] != 'IFRS Servigral']

        # Agregar los datos al DataFrame principal
        datos_ventas = pd.concat([datos_ventas, df], ignore_index=True)
        print(f'Archivo procesado: {archivo}')
    except Exception as e:
        print(f'Error al procesar el archivo {archivo}: {e}')

# 6. Verificar si hay datos cargados
if datos_ventas.empty:
    print("No se cargaron datos. Verifica los archivos.")
    exit()

print(f"Total de registros después de la carga inicial: {len(datos_ventas)}")

# 7. Renombrar columnas para facilitar su uso
datos_ventas.rename(columns={
    'Nbre.Complet.Empresa': 'Empresa',
    'FACTURA': 'Factura',
    'FECHA': 'Fecha',
    'PROVEEDOR': 'Proveedor',
    'SUB ZONA': 'Sub_Zona',
    'ZONA': 'Zona',
    'LINEA DE NEGOCIO': 'Linea_Negocio',
    'TIPO ( PRODUCTO)': 'Tipo_Producto',
    'No. de Parte': 'SKU',
    'Descripc. de Parte': 'Descripcion_Producto',
    'Familia': 'Codigo_Familia',
    'Descripc.de Familia': 'Descripcion_Familia',
    'Cantidad': 'Cantidad',
    'Ingreso por Venta': 'Ingreso_Venta',
    'Bodega Despacho': 'Codigo_Bodega',
    'Año': 'Año',
    'Mes': 'Mes'
}, inplace=True)

# Asegurar que 'SKU' y 'Codigo_Bodega' sean de tipo texto
datos_ventas['SKU'] = datos_ventas['SKU'].astype(str)
datos_ventas['Codigo_Bodega'] = datos_ventas['Codigo_Bodega'].astype(str)

# 8. Convertir las columnas de fecha al tipo datetime
datos_ventas['Fecha'] = pd.to_datetime(datos_ventas['Fecha'], errors='coerce')

# 9. Eliminar filas con valores nulos en columnas clave
columnas_clave = ['Fecha', 'Cantidad', 'Ingreso_Venta']
datos_ventas.dropna(subset=columnas_clave, inplace=True)
print(f"Registros después de eliminar filas con nulos en columnas clave: {len(datos_ventas)}")

# 10. Convertir columnas numéricas al tipo entero
columnas_numericas = ['Cantidad', 'Ingreso_Venta']
for col in columnas_numericas:
    datos_ventas[col] = pd.to_numeric(datos_ventas[col], errors='coerce').fillna(0).astype(int)

# 11. Eliminar filas con valores nulos en columnas numéricas
datos_ventas.dropna(subset=columnas_numericas, inplace=True)
print(f"Registros después de eliminar filas con nulos en columnas numéricas: {len(datos_ventas)}")

# 12. Ordenar los datos por fecha
datos_ventas.sort_values(by='Fecha', inplace=True)

# 13. Aplicar el mapeo de SKU nuevo solo a AGRAC
# Leer el archivo de mapeo de SKU con todas las columnas necesarias
ruta_mapeo_sku = r'C:\ruta\al\archivo\mapeo_sku.xlsx'  # Reemplaza con la ruta real

try:
    mapeo_sku_df = pd.read_excel(ruta_mapeo_sku)
except Exception as e:
    print(f"Error al leer el archivo de mapeo SKU: {e}")
    exit()

# Convertir 'SKU INICIAL' y 'SKU FINAL' a texto en el mapeo
mapeo_sku_df['SKU INICIAL'] = mapeo_sku_df['SKU INICIAL'].astype(str)
mapeo_sku_df['SKU FINAL'] = mapeo_sku_df['SKU FINAL'].astype(str)

# Filtrar el mapeo para SKU de AGRAC
mapeo_sku_agrac = mapeo_sku_df[mapeo_sku_df['COMPAÑÍA'] == 'AGRAC']

# Crear un diccionario de mapeo SKU INICIAL -> SKU FINAL para AGRAC
mapeo_sku = dict(zip(mapeo_sku_agrac['SKU INICIAL'], mapeo_sku_agrac['SKU FINAL']))

# Reemplazar SKU en datos_ventas para registros de AGRAC
condicion_agrac = datos_ventas['Empresa'].isin(['IFRS Agrac', 'IFRS AGSE SAS'])
datos_ventas.loc[condicion_agrac, 'SKU'] = datos_ventas.loc[condicion_agrac, 'SKU'].replace(mapeo_sku)
print(f"Registros después de aplicar el mapeo de SKU para AGRAC: {len(datos_ventas)}")

# 14. Homologar datos categóricos para todos los registros
# Seleccionar las columnas homologadas que necesitamos
columnas_homologadas = [
    'SKU FINAL', 'MARCA', 'INGREDIENTE ACTIVO 1', 'INGREDIENTE ACTIVO 2',
    'UNIDAD MEDIDA', 'LINEA NEGOCIO PROVEEDOR', 'NOMBRE MACRO-FAMILIA',
    'PROVEEDOR GIOVANNA HOMOLOGADO', 'DESCRIPCION FAMILIA'
]

# Eliminar duplicados en el mapeo para evitar problemas al hacer merge
mapeo_categorias = mapeo_sku_df[columnas_homologadas].drop_duplicates(subset=['SKU FINAL'])

# Renombrar 'SKU FINAL' a 'SKU' para hacer el merge
mapeo_categorias.rename(columns={'SKU FINAL': 'SKU'}, inplace=True)

# Asegurarse de que 'SKU' es texto en mapeo_categorias
mapeo_categorias['SKU'] = mapeo_categorias['SKU'].astype(str)

# Hacer el merge de datos_ventas con mapeo_categorias en base a 'SKU'
datos_ventas = datos_ventas.merge(mapeo_categorias, on='SKU', how='left')
print(f"Registros después de hacer el merge con mapeo de categorías: {len(datos_ventas)}")

# Reemplazar las columnas originales en datos_ventas con las homologadas
datos_ventas['Marca'] = datos_ventas['MARCA']
datos_ventas['Ingrediente_Activo_1'] = datos_ventas['INGREDIENTE ACTIVO 1']
datos_ventas['Ingrediente_Activo_2'] = datos_ventas['INGREDIENTE ACTIVO 2']
datos_ventas['Unidad_Medida'] = datos_ventas['UNIDAD MEDIDA']
datos_ventas['Linea_Negocio_Proveedor'] = datos_ventas['LINEA NEGOCIO PROVEEDOR']
datos_ventas['Nombre_Macro_Familia'] = datos_ventas['NOMBRE MACRO-FAMILIA']
datos_ventas['Proveedor_Homologado'] = datos_ventas['PROVEEDOR GIOVANNA HOMOLOGADO']
datos_ventas['Descripcion_Familia'] = datos_ventas['DESCRIPCION FAMILIA']

# Eliminar las columnas intermedias que no necesitamos
datos_ventas.drop(columns=[
    'MARCA', 'INGREDIENTE ACTIVO 1', 'INGREDIENTE ACTIVO 2',
    'UNIDAD MEDIDA', 'LINEA NEGOCIO PROVEEDOR', 'NOMBRE MACRO-FAMILIA',
    'PROVEEDOR GIOVANNA HOMOLOGADO', 'DESCRIPCION FAMILIA'
], inplace=True)

# 15. Unificar los códigos de almacenes satélites
mapeo_almacenes = {
    '5515': '5510',
    '5516': '5510',
    '5533': '5532',
    '5556': '5557',
    '5560': '5557',
    '5573': '5572'
}

# Reemplazar los valores de 'Codigo_Bodega' para los almacenes satélites
datos_ventas['Codigo_Bodega'] = datos_ventas['Codigo_Bodega'].replace(mapeo_almacenes)
print(f"Registros después de unificar códigos de almacenes satélites: {len(datos_ventas)}")

# Asegurar que 'Codigo_Bodega' sea de tipo texto
datos_ventas['Codigo_Bodega'] = datos_ventas['Codigo_Bodega'].astype(str)

# 16. Filtrar los almacenes específicos para el análisis
almacenes_especificos = [
    '5510', '5512', '5513', '5530', '5531', '5532',
    '5542', '5552', '5554', '5555', '5557', '5570',
    '5572', '5585', '5587', '5588'
]

# Filtrar solo los registros con los códigos de almacenes específicos
datos_ventas = datos_ventas[datos_ventas['Codigo_Bodega'].isin(almacenes_especificos)]
print(f"Registros después de filtrar por almacenes específicos: {len(datos_ventas)}")

# Mostrar cantidad de SKUs y almacenes después del filtro
print("Número de almacenes después del filtro:", datos_ventas['Codigo_Bodega'].nunique())
print("Número de SKUs después del filtro:", datos_ventas['SKU'].nunique())

# 17. Filtrar las familias específicas utilizando 'Descripcion_Familia'
# Normalizar nombres para evitar problemas de mayúsculas, acentos y espacios
datos_ventas['Descripcion_Familia'] = datos_ventas['Descripcion_Familia'].str.upper().str.strip()

# Eliminar acentos
datos_ventas['Descripcion_Familia'] = datos_ventas['Descripcion_Familia'].apply(lambda x: unidecode(x) if isinstance(x, str) else x)

familias_especificas = [
    "HERBICIDAS", "INSECTICIDAS", "FUNGICIDAS", "FERTILIZANTES SOLIDOS",
    "FERTILIZANTES LIQUIDOS", "COADYUVANTES", "BIOLOGICOS",
    "MEDICAMENTOS E INSTRUMENTAL", "SEMILLAS SEMESTRALES", "SEMILLAS VEGETALES"
]

# Normalizar la lista de familias específicas
familias_especificas = [unidecode(fam.upper().strip()) for fam in familias_especificas]

# Aplicar el filtro usando 'Descripcion_Familia'
datos_ventas = datos_ventas[datos_ventas['Descripcion_Familia'].isin(familias_especificas)]
print(f"Registros después de filtrar por familias específicas: {len(datos_ventas)}")

# Mostrar cantidad de SKUs y almacenes después del filtro
print("Número de almacenes después del filtro de familias:", datos_ventas['Codigo_Bodega'].nunique())
print("Número de SKUs después del filtro de familias:", datos_ventas['SKU'].nunique())

# Verificar que datos_ventas no esté vacío después del filtrado
if datos_ventas.empty:
    print("No hay datos después de filtrar por familias específicas. Verifica los filtros.")
    exit()

# 18. Guardar el DataFrame consolidado y limpio después de los filtros
ruta_salida = r'C:\ruta\de\salida'  # Reemplaza con la ruta donde quieres guardar el archivo
os.makedirs(ruta_salida, exist_ok=True)
ruta_archivo_salida = os.path.join(ruta_salida, 'datos_ventas_filtrados.xlsx')
datos_ventas.to_excel(ruta_archivo_salida, index=False)
print(f"Archivo 'datos_ventas_filtrados.xlsx' generado con éxito en la ruta: {ruta_archivo_salida}")
