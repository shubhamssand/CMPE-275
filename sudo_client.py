from concurrent import futures
import time
import logging
import threading
import grpc
import sys
sys.path.append('./Gen')
import fileService_pb2
import fileService_pb2_grpc
_ONE_DAY_IN_SECONDS = 60 * 60 * 24

CHUNK_SIZE= 1024*1024

def get_file_chunks(filename):
    with open(filename, 'rb') as f:
        while True:
            piece = f.read(CHUNK_SIZE);
            if len(piece) == 0:
                return
            print('grrrk: new chunk')
            yield fileService_pb2.FileData(username='akshay', filename= filename,data=piece)

def save_chunks(chunks, filename):
    i=0
    global mycursor 
    global query
    global cnx
    global data
    for chunk in chunks:
        with open(filename+str(i), 'wb') as f:
            if data.get(filename,None)==None:
                data[filename]=[0]*2
            data[filename][0]+=1
            i=i+1
            f.write(chunk.data)
    args= (1, filename, i)
    mycursor.execute("Insert into chunk_data VALUES(%d , '%s',  %d)" %(1, filename, i))
    cnx.commit()
    cnx.close()
    print(data[filename][0])



def client():
    while True: 
        choice= int(input("What operation: 1. Upload 2. Download"))
        if choice==1:
            fileName=input("FileName to be uploaded: ")
            channel = grpc.insecure_channel('localhost:3000')
            stub = fileService_pb2_grpc.FileserviceStub(channel)
            response = stub.UploadFile(get_file_chunks(fileName))
            print(response)
        elif choice==2:
            name= input("Name of file to download")
            channel = grpc.insecure_channel('localhost:3000')
            stub = fluffy_pb2_grpc.FileserviceStub(channel)
            response = stub.DownloadFile(fluffy_pb2.FileInfo(fileName=name))
            save_chunks_to_file(response, "downloads/"+name)
            print("File downloaded. ")
        


if __name__ == '__main__':
    client()
