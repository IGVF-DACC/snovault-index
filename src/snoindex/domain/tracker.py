class MessageTracker:


    def __init__(self):
        self.clear_stats()
        self.clear()

    def clear_stats(self):
        self.number_all_messages = 0
        self.number_handled_messages = 0
        self.number_failed_messages = 0

    def clear(self):
        self.new_messages = []
        self.handled_messages = []
        self.failed_messages = []

    def add_new_messages(self, messages):
        self.number_all_messages += len(messages)
        self.new_messages.extend(messages)

    def add_handled_messages(self, messages):
        self.number_handled_messages += len(messages)
        self.handled_messages.extend(messages)

    def add_failed_messages(self, messages):
        self.number_failed_messages += len(messages)
        self.failed_messages.extend(messages)

    def stats(self):
        return {
            'all': self.number_all_messages,
            'handled': self.number_handled_messages,
            'failed': self.number_failed_messages,
        }
