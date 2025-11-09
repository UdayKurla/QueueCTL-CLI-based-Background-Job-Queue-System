from datetime import datetime, timezone

class Job:
    """Represents a job in the queuectl system."""
    
    def __init__(self, id: str, command: str, max_retries: int = 3, **kwargs):
        self.id = id
        self.command = command
        self.max_retries = max_retries
        
        # State and attempt tracking [cite: 24, 25]
        self.state = kwargs.get('state', 'pending') 
        self.attempts = kwargs.get('attempts', 0)
        
        # Timestamps [cite: 25]
        now_iso = datetime.now(timezone.utc).isoformat()
        self.created_at = kwargs.get('created_at', now_iso)
        self.updated_at = kwargs.get('updated_at', now_iso)
        
        # Retry tracking
        self.retry_after_time = kwargs.get('retry_after_time', None) 

    def to_dict(self):
        """Converts job object to a dictionary for storage/display."""
        return self.__dict__

    @classmethod
    def from_dict(cls, data: dict):
        """Creates a Job object from stored data."""
        return cls(data['id'], data['command'], **data)