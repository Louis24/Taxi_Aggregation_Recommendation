import jaydebeapi

url = 'jdbc:phoenix:192.168.58.56:2181'
dirver = 'org.apache.phoenix.jdbc.PhoenixDriver'
params = {'phoenix.schema.isNamespaceMappingEnabled': 'true'}
jarFile = "phoenix-client-4.14.0-cdh5.13.2-shaded.jar"
conn = jaydebeapi.connect(dirver, url, params, jarFile)
curs = conn.cursor()
curs.execute('SELECT * FROM taxi_location LIMIT 1')
result = curs.fetchall()
print(result)
