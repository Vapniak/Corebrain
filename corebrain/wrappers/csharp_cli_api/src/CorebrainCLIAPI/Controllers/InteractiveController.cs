using CorebrainCS;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace CorebrainCLIAPI.Controllers;

[ApiController]
[Route("api/interactive")]
public class InteractiveController : ControllerBase
{
    public static readonly ConcurrentDictionary<string, InteractiveSession> _sessions = new();
    
    private readonly CorebrainCS.CorebrainCS _corebrain;

    public InteractiveController(CorebrainCS.CorebrainCS corebrain)
    {
        _corebrain = corebrain;
    }

    [HttpPost("start")]
    public IActionResult StartSession()
    {
        var sessionId = Guid.NewGuid().ToString();
        var session = _corebrain.StartInteractiveSession();
        _sessions.TryAdd(sessionId, session);
        return Ok(new { SessionId = sessionId });
    }

    [HttpPost("{sessionId}/command")]
    public IActionResult SendCommand(
        [FromRoute] string sessionId,
        [FromBody] string command)
    {
        if (!_sessions.TryGetValue(sessionId, out var session))
            return NotFound("Session not found");

        session.SendCommand(command);
        return Ok(session.GetOutput());
    }

    [HttpDelete("{sessionId}")]
    public IActionResult EndSession([FromRoute] string sessionId)
    {
        if (_sessions.TryRemove(sessionId, out var session))
        {
            session.Dispose();
            return NoContent();
        }
        return NotFound();
    }
    
    // Helper for middleware
    public static bool SessionExists(string sessionId)
    {
        return _sessions.ContainsKey(sessionId);
    }
}