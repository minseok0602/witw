import com.sun.jdi.Bootstrap;
import com.sun.jdi.VirtualMachine;
import com.sun.jdi.VMDisconnectedException;
import com.sun.jdi.ObjectReference;
import com.sun.jdi.StringReference;
import com.sun.jdi.connect.AttachingConnector;
import com.sun.jdi.event.MethodExitEvent;
import com.sun.jdi.request.EventRequest;

/** Test-only debugger. Suspends the real JVM exactly at the requested method exit. */
class EdgeProbe {
    public static void main(String[] args) throws Exception {
        AttachingConnector connector = Bootstrap.virtualMachineManager().attachingConnectors().stream()
                .filter(c -> c.name().equals("com.sun.jdi.SocketAttach")).findFirst().orElseThrow();
        var options = connector.defaultArguments();
        options.get("hostname").setValue(args[0]);
        options.get("port").setValue("5005");
        options.get("timeout").setValue("15000");
        VirtualMachine vm = connector.attach(options);
        var request = vm.eventRequestManager().createMethodExitRequest();
        request.addClassFilter(args[1]);
        request.setSuspendPolicy(EventRequest.SUSPEND_ALL);
        request.enable();
        System.out.println("ARMED");
        System.out.flush();
        try {
            while (true) {
                var events = vm.eventQueue().remove(180000);
                if (events == null) throw new IllegalStateException("Probe timed out");
                boolean hit = false;
                for (var event : events) {
                    if (event instanceof MethodExitEvent exit && exit.method().name().equals(args[2])) {
                        var argument = (ObjectReference) exit.thread().frame(0).getArgumentValues().getFirst();
                        String eventId;
                        if (args[2].equals("consume")) {
                            var json = (StringReference) argument.getValue(argument.referenceType().fieldByName("value"));
                            eventId = json.value().contains("\"event_id\":\"" + args[3] + "\"") ? args[3] : "";
                        } else {
                            var value = argument.getValue(argument.referenceType().fieldByName("eventId"));
                            eventId = value instanceof StringReference text ? text.value() : "";
                        }
                        if (!eventId.equals(args[3])) continue;
                        System.out.println("HIT " + args[2] + " " + eventId);
                        System.out.flush();
                        hit = true;
                    }
                }
                if (!hit) events.resume();
            }
        } catch (VMDisconnectedException expected) {
            System.out.println("DISCONNECTED");
        }
    }
}
