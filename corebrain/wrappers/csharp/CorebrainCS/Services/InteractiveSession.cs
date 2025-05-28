using System.Diagnostics;
using System.Text;

namespace CorebrainCS;

public class InteractiveSession : IDisposable
{
    private readonly Process _process;
    private readonly StreamWriter _stdin;
    private readonly StringBuilder _outputBuffer = new();
    private readonly bool _verbose;

    public InteractiveSession(string pythonPath, string scriptPath, bool verbose)
    {
        _verbose = verbose;
        _process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = pythonPath,
                Arguments = $"\"{scriptPath}\" --interactive",
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            }
        };
        
        _process.Start();
        _stdin = _process.StandardInput;
        _process.BeginOutputReadLine();
        _process.OutputDataReceived += (_, e) => 
        {
            if (!string.IsNullOrEmpty(e.Data)) 
            {
                _outputBuffer.AppendLine(e.Data);
                if (_verbose) Console.WriteLine($"[INTERACTIVE] {e.Data}");
            }
        };
    }

    public void SendCommand(string command)
    {
        _stdin.WriteLine(command);
        _stdin.Flush();
    }

    public string GetOutput()
    {
        var result = _outputBuffer.ToString();
        _outputBuffer.Clear();
        return result.Trim();
    }

    public void Dispose()
    {
        _process.Kill();
        _process.Dispose();
    }
}