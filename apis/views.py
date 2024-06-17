from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
import threading
import time
import os
import datetime

def init_repo():
    if not os.path.exists('.vcs'):
        os.mkdir('.vcs')
        print("Initialized repository.")

init_repo()

def commit(message, data, author):
    commit_dir1 = os.path.join('.vcs', author)
    if not os.path.exists(commit_dir1):
        os.mkdir(commit_dir1)

    commit_dir = os.path.join(commit_dir1, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
    os.mkdir(commit_dir)
    
    # Create a file to store the data
    data_file = os.path.join(commit_dir, 'data.txt')
    with open(data_file, 'w') as f:
        f.write(data)
    
    metadata_file = os.path.join(commit_dir, 'metadata.txt')
    with open(metadata_file, 'w') as f:
        f.write(f"Commit message: {message}\n")
        f.write(f"Author: {author}\n")
        f.write(f"Timestamp: {datetime.datetime.now()}\n")

    print("Committed changes.")


def list_commits(author):
    commit_dir = os.path.join('.vcs', author) 
    if not os.path.exists(commit_dir):
        print("Repository has not been initialized.")
        return

    commit_list = os.listdir(commit_dir)
    if not commit_list:
        print("No commits found in the repository.")
        return

    commits = []

    for commit in commit_list:
        commit_path = os.path.join(commit_dir, commit)
        data_file = os.path.join(commit_path, 'data.txt')
        metadata_file = os.path.join(commit_path, 'metadata.txt')
        if os.path.exists(data_file) and os.path.exists(metadata_file):
            with open(metadata_file, 'r') as meta_f:
                meta_lines = meta_f.readlines()
                date = meta_lines[2].strip().split(': ')[1]
                message = meta_lines[0].strip().split(': ')[1]
                author = meta_lines[1].strip().split(': ')[1]

            with open(data_file, 'r') as data_f:
                data = data_f.read()

            commit_info = [message, date,author, data]
            commits.append(commit_info)
    return commits



def encoding(s1, output):
    # print("Encoding")
    table = {chr(i): i for i in range(256)}
    p, c = "", ""
    p += s1[0]
    code = 256
    output_code = []

    for i in range(len(s1)):
        if i != len(s1) - 1:
            c += s1[i + 1]
        if p + c in table:
            p += c
        else:
            output_code.append(table[p])
            table[p + c] = code
            code += 1
            p = c
        c = ""
    output_code.append(table[p])
    output.append(output_code)

def decoding(op):
    # print("\nDecoding")
    table = {i: chr(i) for i in range(256)}
    old, n = op[0], 0
    s = table[old]
    c = s[0]
    # print(s, end="")
    count = 256
    out = ""
    out = out + s
    for i in range(len(op) - 1):
        n = op[i + 1]
        if n not in table:
            s = table[old]
            s += c
        else:
            s = table[n]
        # print(s, end="")
        out = out + s
        c = s[0]
        table[count] = table[old] + c
        count += 1
        old = n
    return out


@api_view(['POST'])
def getUserData(request):

    if request.method == 'POST':
        user_data = request.data.get("user",'')
        if user_data:
            commit_info_list = list_commits(user_data)
            final = []
            for commit_info in commit_info_list:
                message, date,author, data = commit_info
                mylist = []
                mylist.append(message)
                mylist.append(date)
                mylist.append(author)
                new_int_list = list(map(int, data.split()))
                mylist.append(decoding(new_int_list))
                final.append(mylist)
                            
            # print(commit_info_list)
            return Response(final, status=status.HTTP_200_OK) 
        else:
            print("no user found")
            return Response({'error': 'user not found'}, status=status.HTTP_400_BAD_REQUEST)

    return Response({'error': 'we only deal in post requests'}, status=status.HTTP_400_BAD_REQUEST)


@api_view(['POST'])
def encrypt(request):
    if request.method == 'POST':
        text_data = request.data.get("text", '')
        commit_message = request.data.get("commit",'')
        user_data = request.data.get("user", '')

        if commit_message:
            if text_data:
                if user_data:
                    
                    output = []
                    thread = threading.Thread(target=encoding, args=(text_data, output))
                    thread.start()
                    thread.join()
                    int_str = ' '.join(map(str, output[0]))
                    commit(commit_message,int_str,user_data)


                    response_data = {'message': "data saved successfully"}
                    return Response(response_data, status=status.HTTP_200_OK)                   
                else:
                    print("no user found")
                    return Response({'error': 'no user found'}, status=status.HTTP_400_BAD_REQUEST)
                
            else:
                return Response({'error': 'No data found'}, status=status.HTTP_400_BAD_REQUEST)
            
        else:
            return Response({'error': 'No commit message found'}, status=status.HTTP_400_BAD_REQUEST)
        
    return Response({'error': 'We only deal in post requests'}, status=status.HTTP_400_BAD_REQUEST)


@api_view(['POST'])
def decrypt(request):
    if request.method == 'POST':

        text_data = request.data.get("text", '')

        if text_data:
            response_data = {'processed_text': text_data}
            return Response(response_data, status=status.HTTP_200_OK)
        
    return Response({'error': 'Invalid JSON data.'}, status=status.HTTP_400_BAD_REQUEST)