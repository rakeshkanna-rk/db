class Query:
    def __init__(self):
        self.criteria = []

    def key(self, key_path):
        """Sets the key for the current condition."""
        self.current_key = key_path
        return self

    def __eq__(self, value):
        """Defines equality condition."""
        self.criteria.append((self.current_key, lambda x: x == value))
        return self

    def __ne__(self, value):
        """Defines inequality condition."""
        self.criteria.append((self.current_key, lambda x: x != value))
        return self

    def __gt__(self, value):
        """Defines greater-than condition."""
        self.criteria.append((self.current_key, lambda x: x > value))
        return self

    def __lt__(self, value):
        """Defines less-than condition."""
        self.criteria.append((self.current_key, lambda x: x < value))
        return self

    def __ge__(self, value):
        """Defines greater-than-or-equal condition."""
        self.criteria.append((self.current_key, lambda x: x >= value))
        return self

    def __le__(self, value):
        """Defines less-than-or-equal condition."""
        self.criteria.append((self.current_key, lambda x: x <= value))
        return self

    def AND(self, *queries):
        """Combines multiple queries with AND logic."""
        for query in queries:
            self.criteria.extend(query.criteria)
        return self

    def OR(self, *queries):
        """Combines multiple queries with OR logic, checking any match."""
        def or_match(item):
            return any(query.matches(item) for query in queries)
        
        self.criteria = [(None, or_match)]
        return self

    def matches(self, item):
        """Checks if an item matches all criteria."""
        for key_path, check in self.criteria:
            if key_path is None:
                # Use custom OR condition if key_path is None
                return check(item)
            
            # Navigate through nested keys
            keys = key_path.split('.')
            value = item
            for key in keys:
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    value = None
                    break
            if value is None or not check(value):
                return False
        return True

