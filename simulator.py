
import sqlite3
import atexit

# connect to the database
_conn = sqlite3.connect('world.db')

counter_task_id = 1


# register a function to be called immediately when the interpreter terminates
def _close_db():
    _conn.commit()
    _conn.close()


atexit.register(_close_db)


# our application API:
def print_all_db():
    cur = _conn.cursor()

    cur.execute("""
        SELECT *
        FROM tasks
    """)
    print (cur.fetchall())

def getWorker():
    cur = _conn.cursor()
    cur.execute("""
                SELECT id, name, status 
                FROM workers
            """)
    return cur.fetchall()


def isExistTasks():
    cur = _conn.cursor()
    cur.execute("""SELECT count(*) 
                              FROM tasks
                """)
    sumTask = int(cur.fetchall()[0][0])
    if sumTask > 0:
        return True
    return False


def getTasks(idWorker):
    cur = _conn.cursor()

    cur.execute("SELECT id, task_name, time_to_make, resource_name, resource_amount FROM tasks WHERE worker_id=(?)", (idWorker,))
    return cur.fetchall()

def useTaskResource(taskId):
    cur = _conn.cursor()
    cur.execute("SELECT resource_name, resource_amount FROM tasks WHERE id=(?)", (taskId,))
    val = cur.fetchone()
    taskResourceName = val[0]
    taskResourceAmountReqire = val[1]
    cur.execute("SELECT amount FROM resources WHERE name=(?)", (taskResourceName,))
    resourceAmount = cur.fetchone()[0]
    if taskResourceAmountReqire <= resourceAmount:
        resourceAmount -= taskResourceAmountReqire
        taskResourceAmountReqire = 0
    else:
        taskResourceAmountReqire -= resourceAmount
        resourceAmount = 0
    _conn.execute("UPDATE tasks SET resource_amount = (?) WHERE id=(?)", (taskResourceAmountReqire, taskId,))
    _conn.execute("UPDATE resources SET amount = (?) WHERE name=(?)", (resourceAmount, taskResourceName,))


def isReqireResource(taskId):
    cur = _conn.cursor()
    cur.execute("SELECT resource_amount FROM tasks WHERE id=(?)", (taskId,))
    val = cur.fetchone()
    taskResourceAmount = val[0]
    if taskResourceAmount == 0:
        return False
    return True



def isReqireMakeTask(taskId):
    cur = _conn.cursor()
    cur.execute("SELECT time_to_make FROM tasks WHERE id=(?)", (taskId,))
    val = cur.fetchone()
    taskResourceTTM = val[0]
    if taskResourceTTM == 0:
        return False
    return True


def makeTask(taskId):
    _conn.execute("UPDATE tasks SET time_to_make = time_to_make - 1 WHERE id=(?)", (taskId,))

def setWorkerStatus(workerId, status):
    _conn.execute("UPDATE workers SET status = (?) WHERE id=(?)", (status, workerId,))

def copletedTask (taskId):
    _conn.execute("DELETE FROM tasks WHERE id=(?)", (taskId,))





while True:
    workerList = getWorker()
    for worker in workerList:

        workerId = worker[0]
        workerName = worker[1].strip()
        workerStatus = worker[2]

        taskList = getTasks(workerId)

        sumTaskForWorker = len(taskList)

        if workerStatus == "idle" and sumTaskForWorker == 0:
            continue

        elif workerStatus == "idle" and sumTaskForWorker > 0:
            print(str(workerName), " says: work work")
            setWorkerStatus(workerId, "busy")

        elif workerStatus == "busy" and sumTaskForWorker == 0:
            print (workerName, "says: All Done!")
            setWorkerStatus(workerId, "idle")
            if not isExistTasks():
                break


        else:
            firstTask = taskList[0]
            taskId = firstTask[0]
            taskName = firstTask[1]
            if isReqireMakeTask(taskId):
                print(workerName," is busy", taskName, "...")
                makeTask(taskId)          # time_to_make-=1
                useTaskResource(taskId)
            elif (not isReqireMakeTask(taskId)) and (isReqireResource(taskId)):
                print (workerName,"doing a task")
                useTaskResource(taskId)

            if(not isReqireMakeTask(taskId)) and (not isReqireResource(taskId)):
                copletedTask(taskId)




