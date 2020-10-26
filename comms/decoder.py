import json
DELI = chr(4)
START_DEL = '\n\r'
END_DEL = '\r\n'


def extract_buffer(dbuffer, start_index):
    for i in range(start_index, len(dbuffer[0])):
        if dbuffer[0][i:i+len(END_DEL)]==END_DEL:
            ex = dbuffer[0][start_index:i]
            dbuffer[0] = dbuffer[0][i+len(END_DEL):]
            return ex

def check_buffer(dbuffer):
    extracted = []
    while(len(dbuffer[0]) and dbuffer[0][:0-len(END_DEL)]):
        if dbuffer[0][:len(START_DEL)] == START_DEL:
#            print(dbuffer[0][:len(START_DEL)]==START_DEL)
            ex = extract_buffer(dbuffer, len(START_DEL))
            if not ex:
                break
            extracted.append(ex)
    return extracted


