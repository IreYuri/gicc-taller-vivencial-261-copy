# Subir videos en AWS S3

## 1. Requisitos
- Debes tener instalado `https://github.com/conda-forge/miniforge` y poder invocarlo desde la terminal.

## 2. Archivos a proporcionar
Se te proporcionará:
- Archivo de dependencias YAML denominado `environment.yml`.

## 3. Datos a proporcionar
Datos únicos:
- Nombre de usuario
- Clave de acceso
- Clave secreta

Datos fijos:
- Región: us-east-1 
- Tipo: JSON

## 4. Preparación de la gestión de subida de videos

Los pasos que se detallan en esta sección, solo se ejecutan una sola vez:

Crear entorno desde el archivo YAML:
```
conda env create -f environment.yml
```

Activar entorno recientemente creado:
```
conda activate s3-observatorio
```


Configura tu acceso a AWS con tu usuario:
```
aws configure --profile <tu-usuario>
```



## 5. Subir videos a AWS S3

## 5.1. Subir un video
Asegúrate de ingresar el nombre del video y tu nombre de usuario:
```
python s3_upload.py <nombre_video>.mp4 --bucket observatorio-videos-gicc --profile <tu-usuario>
```

## 5.2. Subir videos contenidos en una carpeta
Asegúrate de ingresar el nombre de la carpeta y tu nombre de usuario:
```
python s3_upload.py <ubicación_carpeta> --bucket observatorio-videos-gicc --profile <tu-usuario>
```

