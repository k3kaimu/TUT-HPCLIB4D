module tuthpc.mpi;

import tuthpc.hosts;
import tuthpc.taskqueue;

import mpi;
import msgpack;
import std.exception;
import std.stdio;
import std.algorithm;
import std.range;


version(TUTHPC_USE_MPI)
{
    shared static this()
    {
        if(!nowRunningOnClusterDevelopmentHost){
            import std.stdio;
            import core.runtime;
            MPI_Init(&(Runtime.cArgs.argc), &(Runtime.cArgs.argv)).checkMPIError();
        }
    }


    shared static ~this()
    {
        if(!nowRunningOnClusterDevelopmentHost){
            MPI_Finalize();
        }
    }
}


void checkMPIError(int error, string file = __FILE__, size_t line = __LINE__)
{
    enforce(error == 0, "MPI Error", file, line);
}


struct MPIMessageHeader
{
    int rank;
    size_t payloadLength;
}


struct MPIMessagePayload
{
    string typename;
    ubyte[] data;
}


struct MPIMessage(T)
{
    int rank;
    T data;
}


//interface IMPICommunicator
//{
//    void barrier();
//    void sendTo(int nodeId, int tag, MPIMessageHeader header, MPIMessagePayload payload);
//    void recvFrom(int nodeId, int tag, MPIMessageHeader* header, MPIMessagePayload* payload);
//}


//struct MPICommunicator(Impl)
//{
//    Impl instance;
//    alias instance this;
//}


//class MPICommunicatorImpl : IMPICommunicator
//{

//}


final class MPICommunicator
{
    import std.exception : enforce;

    this(MPIEnvironment env)
    {
        _env = env;
    }


    void barrier()
    {
        MPI_Barrier(MPI_COMM_WORLD).checkMPIError();
    }


    //T[] gather(T)(T value)


    void sendTo(T)(int nodeId, T data)
    {
        //writefln("[%s/%s] Send Data %s", _env.rank, _env.totalProcess, data);
        MPIMessageHeader header;
        MPIMessagePayload payload;
        auto bytes = msgpack.pack(data);
        payload.data = bytes;
        payload.typename = T.stringof;

        auto payloadBytes = msgpack.pack(payload);
        header.rank = _env.rank;
        header.payloadLength = payloadBytes.length;

        enforce(MPIMessageHeader.sizeof < int.max);
        enforce(bytes.length < int.max);

        MPI_Send(&header, cast(int)MPIMessageHeader.sizeof, MPI_BYTE, nodeId, 1, MPI_COMM_WORLD).checkMPIError();
        MPI_Send(payloadBytes.ptr, cast(int)payloadBytes.length, MPI_BYTE, nodeId, 2, MPI_COMM_WORLD).checkMPIError();
    }


    bool recvFrom(T)(int nodeId, MPIMessage!T* dst = null)
    {
        if(_received is null){
            MPIMessageHeader header; header.rank = -1;
            MPI_Status istatus;
            MPI_Recv(&header, cast(int)MPIMessageHeader.sizeof, MPI_BYTE, nodeId, 1, MPI_COMM_WORLD, &istatus).checkMPIError();
            //writefln("[%s/%s] Receive Header %s", _env.rank, _env.totalProcess, header);

            ubyte[] buf = new ubyte[header.payloadLength];
            enforce(header.payloadLength < int.max);

            if(nodeId == MPI_ANY_SOURCE)
                nodeId = header.rank;

            MPI_Recv(buf.ptr, cast(int)header.payloadLength, MPI_BYTE, nodeId, 2, MPI_COMM_WORLD, &istatus).checkMPIError();
            //writefln("[%s/%s] Buffer %s", _env.rank, _env.totalProcess, buf);
            MPIMessagePayload payload = msgpack.unpack!MPIMessagePayload(buf);
            //writefln("[%s/%s] Receive Payload %s", _env.rank, _env.totalProcess, payload);
            _received = new MPIMessageVariant(header.rank, payload.typename, payload.data);
        }

        if(_received.typename == T.stringof && (nodeId == MPI_ANY_SOURCE || _received.rank == nodeId)){
            if(dst !is null){
                dst.rank = _received.rank;
                dst.data = msgpack.unpack!T(_received.data);
                //writefln("[%s/%s] Receive %s", _env.rank, _env.totalProcess, dst.data);

                _received = null;
            }

            return true;
        }else
            return false;
    }


    bool recvFromAny(T)(MPIMessage!T* dst = null)
    {
        return recvFrom!T(MPI_ANY_SOURCE, dst);
    }


  private:
    MPIEnvironment _env;
    MPIMessageVariant* _received;

    static struct MPIMessageVariant
    {
        int rank;
        string typename;
        ubyte[] data;
    }
}


final class MPIEnvironment
{
    private
    this()
    {
        enforce(_instance is null);
    }


    bool isMaster() const @property { return _rank == 0; }
    bool isWorker() const @property { return !this.isMaster; }


    int rank() const @property { return _rank; }
    int totalProcess() const @property { return _totalProcess; }


    static
    MPIEnvironment instance() @property
    {
        if(_instance is null){
            _instance = new MPIEnvironment;
            MPI_Comm_rank(MPI_COMM_WORLD, &(_instance._rank));
            MPI_Comm_size(MPI_COMM_WORLD, &(_instance._totalProcess));

            import std.stdio, std.socket;
            //writefln("Init: [%s/%s] %s", _instance._rank, _instance._totalProcess, Socket.hostName);
        }

        return _instance;
    }


  private:
    int _rank;
    int _totalProcess;

  static:
    MPIEnvironment _instance;
}


final class MPITaskScheduler
{
    import std.exception : enforce;
    import std.algorithm : any, all;
    import std.range;


    this()
    {
        _env = MPIEnvironment.instance;
        _comm = new MPICommunicator(_env);

        if(_env.isMaster)
            foreach(i; 1 .. _env.totalProcess)
                _workers[i] = WorkerStatus.waiting;
    }


    MPIEnvironment environment() @property { return _env; }
    MPICommunicator communicator() @property { return _comm; }


    void syncAllProcess()
    {
        _comm.barrier();
    }


    void runOnlyMaster(void delegate() dg)
    {
        if(_env.isMaster){
            dg();
        }
    }


    void runOnlyMasterWithSync(void delegate() dg)
    {
        syncAllProcess();
        runOnlyMaster(dg);
        syncAllProcess();
    }


    void runImpl(alias func, R, DArgs...)(lazy R argsList, DArgs defaultArgs)
    {
        _comm.barrier();
        if(_env.isMaster){
            //writefln("[%s/%s] Is Master", _env.rank, _env.totalProcess);
            auto ctrl = runMasterProcess(argsList);
            if(ctrl == ControlData.terminated)
                throw new Exception("Terminated Master Process");
        }
        else{
            //writefln("[%s/%s] Is Worker", _env.rank, _env.totalProcess);
            auto ctrl = runWorkerProcess!(func, ElementType!R)(defaultArgs);
            if(ctrl == ControlData.terminated)
                throw new Exception("");
        }
        _comm.barrier();
    }


    private
    ControlData runMasterProcess(R)(R argsList)
    {
        scope(failure)
        {
            foreach(inode, ref status; _workers)
                _comm.sendTo(inode, ControlData.terminated);
        }

        void sendNextTask(int target)
        {
            //writefln("[%s/%s] Send Next Task To %s", _env.rank, _env.totalProcess, target);
            _comm.sendTo(target, argsList.front);
            _workers[target] = WorkerStatus.running;
            argsList.popFront();
        }


        foreach(inode, ref status; _workers){
            if(argsList.empty) break;
            sendNextTask(inode);
        }

        while(!argsList.empty)
        {
            MPIMessage!Notification info;
            enforce(_comm.recvFromAny(&info), "Unknown received data");
            //writefln("[%s/%s] Receive Notification %s", _env.rank, _env.totalProcess, info);

            if(info.data == Notification.done || info.data == Notification.failure)
                _workers[info.rank] = WorkerStatus.waiting;
            else if(info.data == Notification.terminated){
                _workers[info.rank] = WorkerStatus.terminated;
                if(_workers.byValue.all!(a => a == WorkerStatus.terminated))
                    return ControlData.terminated;
            }else
                throw new Exception("Unknown received data");

            if(_workers[info.rank] == WorkerStatus.waiting)
                sendNextTask(info.rank);
        }

        //writefln("[%s/%s] Workers %s, %s", _env.rank, _env.totalProcess, _workers, _workers.byValue.any!(a => a == WorkerStatus.running));

        while(_workers.byValue.any!(a => a == WorkerStatus.running)){
            MPIMessage!Notification info;
            enforce(_comm.recvFromAny(&info), "Unknown received data");
            //writefln("[%s/%s] Receive Notification %s", _env.rank, _env.totalProcess, info);

            if(info.data == Notification.done || info.data == Notification.failure)
                _workers[info.rank] = WorkerStatus.waiting;
            else if(info.data == Notification.terminated)
                _workers[info.rank] = WorkerStatus.terminated;
            else
                throw new Exception("Unknown received data");
        }


        foreach(inode, status; _workers) if(status == WorkerStatus.waiting){
            _comm.sendTo(inode, ControlData.doneAllTask);
            //writefln("[%s/%s] Send to doneAllTask To %s", _env.rank, _env.totalProcess, inode);
        }

        return ControlData.doneAllTask;
    }


    private
    ControlData runWorkerProcess(alias func, E, Args...)(Args args)
    {
        scope(failure)
        {
            _comm.sendTo(0, Notification.terminated);
        }

        while(1){
            if(_comm.recvFrom!E(0)){
                MPIMessage!E msg;
                _comm.recvFromAny!E(&msg);

                Object obj;
                try
                    func(msg.data, args);
                catch(Exception ex){
                    writeln(ex);
                    obj = ex;
                }
                catch(Error err){
                    writeln(err);
                    obj = err;
                }

                if(obj !is null)
                    _comm.sendTo(0, Notification.failure);
                else
                    _comm.sendTo(0, Notification.done);
            }
            else if(_comm.recvFrom!ControlData(0)){
                MPIMessage!ControlData ctrl;
                _comm.recvFrom(0, &ctrl);
                //writefln("[%s/%s] receive control data %s", _env.rank, _env.totalProcess, ctrl);
                return ctrl.data;
            }
            else{
                throw new Exception("Unknown received data.");
            }
        }
    }


  private:
    MPIEnvironment _env;
    MPICommunicator _comm;
    WorkerStatus[int] _workers;


    enum WorkerStatus : int
    {
        terminated = -1,
        waiting = 0,
        running = 1,
    }


    enum ControlData : int
    {
        terminated = -2,
        failure = -1,
        doneAllTask = 1,
    }


    enum Notification : int
    {
        terminated = -2,
        failure = -1,
        done = 0
    }
}


void run(alias func, R, Args...)(MPITaskScheduler s, lazy R range, Args args)
{
    static
    void runTask(T...)(T args) { func(args); }

    s.runImpl!runTask(range, args);
}


void run(MPITaskScheduler s, MultiTaskList taskList)
{
    import std.range : iota;

    static void runTask(size_t i, MultiTaskList list) { list[i](); }

    s.runImpl!runTask(iota(taskList.length), taskList);
}


void run(MPITaskScheduler s, void delegate()[] taskList)
{
    s.run(new MultiTaskList(taskList));
}

