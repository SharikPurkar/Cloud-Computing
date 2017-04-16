dictionary = {}
path = input("File location :: ")
with open(path+"input.txt") as file:
    line = file.readline()
    while(line!=""):
        page,rank=line.split()
        dictionary[page]=rank
        line = file.readline()

with open(path+"output.txt",'w') as ofile:
    count = 10        
    for entry in sorted(dictionary, key=dictionary.get, reverse = True):
        if count <= 0:
            break
        else:
            ofile.write(entry + "  " + dictionary[entry]+ "\n")
            count -=1
