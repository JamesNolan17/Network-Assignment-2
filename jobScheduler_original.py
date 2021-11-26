from datetime import datetime, timedelta
import time
import socket
import sys
import argparse
import signal
from queue import Queue

SERVER_RECORD = {}
WORK_ASSIGN_RECORD = {}
PENDING_JOB = Queue(maxsize=0)
FILE_SIZE_HISTORY = []

FILESIZE_AVE = 50
DEFAULT_CAPACITY = 50


# KeyboardInterrupt handler
def sigint_handler(signal, frame):
    print('KeyboardInterrupt is caught. Close all sockets :)')
    sys.exit(0)

# send trigger to printAll at servers
def sendPrintAll(serverSocket):
    serverSocket.send(b"printAll\n")

# Parse available severnames
def parseServernames(binaryServernames):
    servernames = binaryServernames.decode().split(',')[:-1]
    for i in servernames:
        SERVER_RECORD[i] = {"capacity": -1, "workload": 0, "unfinished_work": {}}
    return servernames


# get the completed file's name, what you want to do?
def getCompletedFilename(filename):
    ####################################################
    #                      TODO                        #
    # You should use the information on the completed  #
    # job to update some statistics to drive your      #
    # scheduling policy. For example, check timestamp, #
    # or track the number of concurrent files for each #
    # server?                                          #
    ####################################################

    finish_timestamp = time.time()
    server_to_send = WORK_ASSIGN_RECORD[filename]

    # Calculate capacity
    JCT = round((finish_timestamp - SERVER_RECORD[server_to_send]["unfinished_work"][filename][1]), 2)
    file_size = SERVER_RECORD[server_to_send]["unfinished_work"][filename][0]


    # Update capacity using the first file (It is not accurate indeed, but it is helpful to find super slow servers)
    if SERVER_RECORD[server_to_send]["capacity"] == -1:
        if file_size != -1:
            SERVER_RECORD[server_to_send]["capacity"] = file_size / JCT
        else:
            SERVER_RECORD[server_to_send]["capacity"] = FILESIZE_AVE / JCT

    del SERVER_RECORD[server_to_send]["unfinished_work"][filename]
    if file_size != -1:
        SERVER_RECORD[server_to_send]["workload"] -= file_size
    else:
        SERVER_RECORD[server_to_send]["workload"] -= FILESIZE_AVE

    # print message
    print(SERVER_RECORD)
    print(f"[JobScheduler] Filename {filename} is finished. Server{server_to_send}(WL:{SERVER_RECORD[server_to_send]['workload']}):{JCT}s")

# formatting: to assign server to the request
def scheduleJobToServer(servername, request):
    return (servername + "," + request + "\n").encode()


server_choice_id = None
server_spec = None
first_job_dispatching = 0
def first_round(servernames):
    return first_job_dispatching < len(servernames)

# main part you need to do
def assignServerToRequest(servernames, request):
    global server_choice_id
    global server_spec
    global first_job_dispatching
    ####################################################
    #                      TODO                        #
    # Given the list of servers, which server you want #
    # to assign this request? You can make decision.   #
    # You can use a global variables or add more       #
    # arguments.                                       #

    request_name = request.split(",")[0]
    request_size = int(request.split(",")[1])
    request_timestamp = time.time()
    # Find a good server to send to
    # Policy: Find the server with the maximum remaining capacity
    server_choice_id = 0
    server_spec = SERVER_RECORD[servernames[server_choice_id]]
    least_amount_of_work = server_spec["workload"] / server_spec["capacity"]
    can_send = False

    for server_id in range(0, len(servernames)):
        server_spec_candidate = SERVER_RECORD[servernames[server_id]]
        if server_spec_candidate["capacity"] == -1:
            if not first_round(servernames):
                # That means that there are no job finished.
                continue
            else:
                # If it is first time, let the server with unknown capacity win
                capacity = sys.float_info.max
        else:
            capacity = server_spec_candidate["capacity"]
            can_send = True
        least_amount_of_work_candidate = server_spec_candidate["workload"] / capacity
        if abs(least_amount_of_work_candidate) < abs(least_amount_of_work):
            server_choice_id = server_id
            server_spec = server_spec_candidate
            least_amount_of_work = least_amount_of_work_candidate

    server_to_send = servernames[server_choice_id]
    ####################################################

    # Schedule the job
    scheduled_request = scheduleJobToServer(server_to_send, request)
    # If cannot send, put it in Q
    if (not can_send) and not first_round(servernames):
        PENDING_JOB.put(request)
        return b""
    # Record the stats if can send
    SERVER_RECORD[server_to_send]["unfinished_work"][request_name] = (request_size, request_timestamp)
    if request_size == -1:
        SERVER_RECORD[server_to_send]["workload"] += FILESIZE_AVE
    else:
        SERVER_RECORD[server_to_send]["workload"] += request_size
    WORK_ASSIGN_RECORD[request_name] = server_to_send
    # print(f"time:{(time.time() - request_timestamp)*1000}")
    if first_round(servernames):
        first_job_dispatching += 1
        print(f"{first_job_dispatching} time! {server_to_send}")
    return scheduled_request


def parseThenSendRequest(clientData, serverSocket, servernames):
    # print received requests
    print(f"[JobScheduler] Received binary messages:\n{clientData}")
    print(f"--------------------")
    # parsing to "filename, jobsize" pairs
    requests = clientData.decode().split("\n")[:-1]


    sendToServers = b""
    for request in requests:
        if request[0] == "F":
            # if completed filenames, get the message with leading alphabet "F"
            filename = request.replace("F", "")
            getCompletedFilename(filename)  
        else:
            # if requests, add "servername" front of the pairs -> "servername, filename, jobsize"
            sendToServers = sendToServers + \
                assignServerToRequest(servernames, request)

    # send "servername, filename, jobsize" pairs to servers
    if sendToServers != b"":
        serverSocket.send(sendToServers)


if __name__ == "__main__":
    # catch the KeyboardInterrupt error in Python
    signal.signal(signal.SIGINT, sigint_handler)

    # parse arguments and get port number
    parser = argparse.ArgumentParser(description="JobScheduler.")
    parser.add_argument('-port', '--server_port', action='store', type=str, required=True,
                        help='port to server/client')
    args = parser.parse_args()
    server_port = int(args.server_port)

    # open socket to servers
    serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serverSocket.connect(('127.0.0.1', server_port))

    # IMPORTANT: for 50ms granularity of emulator
    serverSocket.settimeout(0.0001)

    # receive preliminary information: servernames (can infer the number of servers)
    binaryServernames = serverSocket.recv(4096)
    servernames = parseServernames(binaryServernames)
    print(f"Servernames: {servernames}")

    currSeconds = -1
    now = datetime.now()
    while (True):
        if PENDING_JOB.qsize() > 0:
            top_delay_request = PENDING_JOB.get()
            assignServerToRequest(servernames, top_delay_request)
        try:
            # receive the completed filenames from server
            completeFilenames = serverSocket.recv(4096)
            if completeFilenames != b"":
                parseThenSendRequest(completeFilenames, serverSocket, servernames)
        except socket.timeout:
            # IMPORTANT: catch timeout exception, DO NOT REMOVE
            pass

        # # Example printAll API : let servers print status in every seconds
        if (datetime.now() - now).seconds > currSeconds:
            currSeconds = currSeconds + 1
            sendPrintAll(serverSocket)
