import requests, io, tempfile, os, boto3
from zipfile import ZipFile

bucket = "landing-zone-vini-etl-aws"
folder_temp_name = 'temp'
url = 'https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2020.zip'
file_name = "microdados_censo_escolar_2020.zip"

with tempfile.TemporaryDirectory() as temp_path:
    temp_dir = os.path.join(temp_path, folder_temp_name)
    with open(temp_dir, 'wb') as f:
        req = requests.get(url)
        f.write(req.content)
    s3 = boto3.resource('s3')
    s3.Bucket(bucket).upload_file(temp_dir, file_name)

    zip_obj = s3.Object(bucket_name=bucket, key=file_name)
    buffer = io.BytesIO(zip_obj.get()["Body"].read())
    
    z = ZipFile(buffer)
    for filename in z.namelist():
        file_info = z.getinfo(filename)
        s3.meta.client.upload_fileobj(
            z.open(filename),
            Bucket=bucket,
            Key='data/' + f'{filename}')


