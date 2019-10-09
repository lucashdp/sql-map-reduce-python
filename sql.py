  
import mincemeat
import glob
import csv

text_files = glob.glob("C:\\userx\\exerc\\join\\*")

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()

source = dict((file_name, file_contents(file_name)) for file_name in text_files)

def mapfn(k, v):
    print ('map ' + k)
    for line in v.splitlines():
        if k == "C:\\userx\\exerc\\join\\2.2-vendas.csv":
            yield line.split(';')[0], "Vendas" + ":" + line.split(";")[5]
        if k == "C:\\userx\\exerc\\join\\2.2-filiais.csv":
            yield line.split(';')[0], "Filial" + ":" + line.split(";")[1]

def reducefn(k, v):
    print ('reduce' + k)
    total = 0
    for index, item in enumerate(v):
        if item.split(":")[0] == 'Vendas':
            total = int(item.split(":")[1]) + total
        elif item.split(":")[0] == "Filial":
            nome_filial = item.split(":")[1]
    
    lista = list()
    lista.append(nome_filial + " , " + str(total))
    return lista

s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

w = csv.writer(open("C:\\userx\\exerc\\join\\result_join.csv", "w"))
for k, v in results.items():
    w.writerow([k, v])