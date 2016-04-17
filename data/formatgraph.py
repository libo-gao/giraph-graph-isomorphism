path = "p2p-Gnutella08.txt"

f1 = file(path, "r")
f3 = file("revised-p2p",'w')
	
line = f1.readline()
tokens = line.split()
curr_node = tokens[0]
newline = "["+curr_node+","+"0,["
while line:
	if(tokens[0]==curr_node):
		newline+="["+tokens[1]+",1],"
	else:
		if(newline[len(newline)-1]==','):
			newline = newline[:len(newline)-1]
		newline+="]]"
		f3.write(newline+'\n')
		curr_node = tokens[0]
		newline = "["+curr_node+","+"0,["+"["+tokens[1]+",1],"
	line = f1.readline()
        tokens = line.split()	
if(newline[len(newline)-1]==','):
	newline = newline[:len(newline)-1]
newline+="]]"
f3.write(newline)
f1.close()
f3.close()
