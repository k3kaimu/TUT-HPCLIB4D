module tuthpc.mpi;

import mpi;

version(TUTHPC_USE_MPI)
{
    shared static this()
    {
        MPI_Init(&(core.runtime.CArgs.argc), &(core.runtime.CArgs.argv)).checkMPIError();
    }


    shared static ~this()
    {
        MPI_Finalize();
    }
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


final class MPICommunicator
{
    this(MPIEnvironment env)
    {
        _env = env;
    }


    void sendTo(T)(int nodeId, T data)
    {
        MPIMessageHeader header;
        MPIPayload payload;
        auto bytes = msgpack.pack(data);
        payload.data = bytes;
        payload.typename = T.stringof;

        auto pyloadBytes = msgpack.pack(payload);
        header.rank = _env.rank;
        header.payloadLength = payloadBytes.length;

        MPI_Send(&header, MPIMessageHeader.sizeof, MPI_CHAR, nodeId, 0, MPI_COMM_WORLD);
        MPI_Send(pyloadBytes.ptr, bytes.length, MPI_CHAR, nodeId, 0, MPI_COMM_WORLD);
    }


    bool recvFrom(T)(ptrdiff_t nodeId, MPIMessage!T* dst = null)
    {
        if(_received is null){
            MPIMessageHeader header;
            MPI_Status istatus;
            MPI_Recv(&header, MPIMessageHeader.sizeof, MPI_CHAR, nodeId, 0, MPI_COMM_WORLD, &istatus);

            ubyte[] buf = new ubyte[header.dataLength];
            MPI_Recv(buf.ptr, header.payloadLength, MPI_CHAR, nodeId, 0, MPI_COMM_WORLD, &istatus);
            MPIMessagePayload payload = msgpack.unpack!MPIMessagePayload(buf);

            _received = new MPIMessageVariant(header.rank, payload.typename, payload.data);
        }

        if(_received.typename == T.stringof && (nodeId == MPI_SOURCE_ANY || _received.rank == nodeId)){
            if(dst !is null){
                dst.rank = _received.rank;
                dst.data = msgpack.unpack!T(_received.data);

                _received = null;
            }

            return true;
        }else
            return false;
    }


    T recvFromAny(T)(MPIMessage!T* dst = null)
    {
        return recvFrom!T(MPI_SOURCE_ANY, dst);
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
    this()
    {
        MPI_Comm_rank(MPI_COMM_WORLD, &_rank);
        MPI_Comm_size(MPI_COMM_WORLD, &_totalProcess);
    }


    bool isMaster() const @property { return _rank == 0; }
    bool isWorker() const @property { return !this.isMaster; }


    int rank() const @property { return _rank; }
    int totalProcess() const @property { return _totalProcess; }


  private:
    int _rank;
    int _totalProcess;
}


final class MPITaskSchedular
{
    this(MPIEnvironment env)
    {
        _env = env;
        _comm = new MPICommunicator(env);

        if(_env.isMaster)
            foreach(i; 1 .. env.totalProcess)
                _workers[i] = WorkerStatus.waiting;
    }


    void runOnMPITaskSchedular(alias func, R)(R argsList)
    {
        if(env.isMaster){
            auto ctrl = runMasterProcess(argsList);
            if(ctrl == ControlData.terminated)
                throw new Exception("Terminated Master Process");
        }
        else{
            auto ctrl = runWorkerProcess!(func, ElementType!R)();
            if(ctrl == ControlData.terminated)
                throw new Exception("")
        }
    }


    ControlData runMasterProcess(R)(R argsList)
    {
        scope(failure)
        {
            foreach(inode, ref status; _workers)
                _comm.sendTo(inode, ControlData.terminated);
        }

        void sendNextTask(int target)
        {
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

            if(info.data == Notification.done || info.data == Notification.failure)
                _workers[ctrl.rank] = WorkerStatus.waiting;
            else if(info.data == Notification.terminated){
                _workers[ctrl.rank] = WorkerStatus.terminated;
                if(_workers.byValues.all!(a => a == WorkerStatus.terminated))
                    return ControlData.terminated;
            }else
                throw new Exception("Unknown received data");

            if(_workers[ctrl.rank] == WorkerStatus.waiting)
                sendNextTask(ctrl.rank);
        }

        while(_workers.byValues.any!(a => a == WorkerStatus.running)){
            MPIMessage!Notification info;
            enforce(_comm.recvFromAny(&info), "Unknown received data");

            if(info.data == Notification.done || info.data == Notification.failure)
                _workers[ctrl.rank] = WorkerStatus.waiting;
            else if(info.data == Notification.terminated)
                _workers[ctrl.rank] = WorkerStatus.terminated;
            else
                throw new Exception("Unknown received data");
        }


        foreach(inode, status; _workers) if(status == WorkerStatus.waiting)
            _comm.sendTo(inode, ControlData.doneAllTask);

        return ControlData.doneAllTask;
    }


    ControlData runWorkerProcess(alias func, E)()
    {
        scope(failure)
        {
            _comm.sendTo(0, Notification.terminated);
        }

        while(1){
            if(_comm.recvFrom!E(0)){
                MPIMessage!E args;
                _comm.recvFromAny!E(&args);

                Object obj;
                try
                    func(args.data);
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
                _comm.recvFrom!(0, &ctrl);
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
