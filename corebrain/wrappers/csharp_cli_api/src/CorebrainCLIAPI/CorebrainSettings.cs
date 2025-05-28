namespace CorebrainCLIAPI;

/// <summary>
/// Represents the configuration settings for the Corebrain CLI wrapper.
/// </summary>
public class CorebrainSettings
{
    public CorebrainSettings() { }

    public string PythonPath { get; set; }
    public string ScriptPath { get; set; }
    public bool Verbose { get; set; }

    public CorebrainSettings(string pythonPath, string scriptPath)
    {
        PythonPath = pythonPath;
        ScriptPath = scriptPath;
    }
}
