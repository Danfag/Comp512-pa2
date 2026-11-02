package comp512st.paxos;

// Access to the GCL layer
import comp512.gcl.*;

import comp512.utils.*;

// Any other imports that you may need.
import java.awt.desktop.AppReopenedEvent;
import java.io.*;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Queue;
import java.util.logging.*;
import java.net.UnknownHostException;


// ANY OTHER classes, etc., that you add must be private to this package and not visible to the application layer.

// extend / implement whatever interface, etc. as required.
// NO OTHER public members / methods allowed. broadcastTOMsg, acceptTOMsg, and shutdownPaxos must be the only visible methods to the application layer.
//		You should also not change the signature of these methods (arguments and return value) other aspects maybe changed with reasonable design needs.
public class Paxos
{
	GCL gcl;
	FailCheck failCheck;
    static volatile int  hostCount=0;
    static volatile int agreedUpon;
    static volatile int potentialAgreement;
    int promises=0;
    int highestObserved=0;
    Queue<String[]> promiseMessages = new ArrayDeque<>();
    Queue<String[]> valMessages = new ArrayDeque<>();

	public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck) throws IOException, UnknownHostException
	{
		// Rember to call the failCheck.checkFailure(..) with appropriate arguments throughout your Paxos code to force fail points if necessary.
		this.failCheck = failCheck;
        hostCount++;

		// Initialize the GCL communication system as well as anything else you need to.
		this.gcl = new GCL(myProcess, allGroupProcesses, null, logger) ;

	}

    private void propose(int potentialAgreement){
        gcl.broadcastMsg("Propose," + potentialAgreement);

    }
    private void promise (int promiseValue){
        gcl.broadcastMsg("Promise," + promiseValue);
    }

	// This is what the application layer is going to call to send a message/value, such as the player and the move
	public void broadcastTOMsg(Object val) throws InterruptedException {
        synchronized ((Object) potentialAgreement) {
        potentialAgreement++;}
        var valueToPropose=potentialAgreement;
        promises=0;
        propose(valueToPropose);
        var prev = agreedUpon;

        while(promises<hostCount/2){
            if(prev!=agreedUpon){
                synchronized((Object) potentialAgreement){
                    potentialAgreement++;
                }
                valueToPropose=potentialAgreement;
                propose(valueToPropose);
                promises=0;
                prev=agreedUpon;
            }
            if(!promiseMessages.isEmpty()){
                var msg = promiseMessages.poll();
                while(Integer.parseInt(msg[1])<potentialAgreement){
                    msg = promiseMessages.poll();
                }
                if(Integer.parseInt(msg[1])>potentialAgreement){
                    return;
                }
                promises++;
                continue;

            }

        }
		// This is just a place holder.
		// Extend this to build whatever Paxos logic you need to make sure the messaging system is total order.
		// Here you will have to ensure that the CALL BLOCKS, and is returned ONLY when a majority (and immediately upon majority) of processes have accepted the value.
		synchronized ((Object)  agreedUpon){
        agreedUpon=valueToPropose;}
        gcl.broadcastMsg(agreedUpon +","+ val );
	}

	// This is what the application layer is calling to figure out what is the next message in the total order.
	// Messages delivered in ALL the processes in the group should deliver this in the same order.
	public Object msgHandler() throws InterruptedException
	{
		// This is just a place holder.
        while(true){
		GCMessage gcmsg = gcl.readGCMessage();
        var msg = gcl.readGCMessage().toString().split(",");
        var msgType=msg[0];
        switch(msgType){
            case "Propose":
                if(Integer.parseInt(msg[1])<highestObserved){
                    highestObserved=Integer.parseInt(msg[1]);
                    promise(Integer.parseInt(msg[1]));
                }
                break;
            case "Promise":
                promiseMessages.add(msg);
                break;
            default:
                if(Integer.parseInt(msg[1])==agreedUpon){
                    valMessages.add(msg);
                }
        }
		return gcmsg.val;
	}}
    public Object acceptTOMsg() throws InterruptedException{
        while(valMessages.isEmpty()){}
        return valMessages.poll();
    }

	// Add any of your own shutdown code into this method.
	public void shutdownPaxos()
	{
		gcl.shutdownGCL();
	}
}

