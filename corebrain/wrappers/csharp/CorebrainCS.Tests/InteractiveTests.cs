using System.Threading;
using Xunit;
using CorebrainCS;

public class InteractiveTests
{
    [Fact]
    public void Should_Handle_MultiStage_Flow()
    {
        var corebrain = new CorebrainCS.CorebrainCS(
            pythonPath: "../../../../venv/Scripts/python.exe",
            scriptPath: "../../../cli",
            verbose: true
        );
        
        using var session = corebrain.StartInteractiveSession();
        
        // Etap 1: Rozpoczęcie konfiguracji
        session.SendCommand("--configure");
        Thread.Sleep(500);
        
        // Etap 2: Wybór typu bazy danych
        session.SendCommand("2"); // MongoDB
        Thread.Sleep(500);
        
        // Etap 3: Podanie connection stringa
        session.SendCommand("mongodb://localhost:27017");
        Thread.Sleep(500);
        
        // Etap 4: Potwierdzenie
        session.SendCommand("Y");
        
        var output = session.GetOutput();
        Assert.Contains("Configuration saved", output);
    }
}