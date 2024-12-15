# Estrutura de tópicos em àrvore
class Topic:

    def __init__(self, name):
        self.name = name
        self.sub_topics = {}
        self.consumers = []
        self.last_value = None

    # subescreve um consumer ao sub-tópico dado pelo topic_path
    def subscribe_topic(self, conn, topic_path):
        if len(topic_path) == 1:
            self.consumers.append(conn)
            return True
        else:
            if topic_path[1] not in self.sub_topics:
                self.sub_topics[topic_path[1]] = Topic(topic_path[1])
            self.sub_topics[topic_path[1]].subscribe_topic(
                conn, topic_path[1:])

    # pesquisa todos os consumers do topico dado pelo caminho e seus sub-tópicos
    def find_consumers(self, topic_path):
        if len(topic_path) == 1:
            return self.consumers

        for st in self.sub_topics:
            if st == topic_path[1]:
                return self.consumers + self.sub_topics[st].find_consumers(topic_path[1:])
        return self.consumers

    def get_all_topic_values(self):
        topic_pair =  [(self.name, self.last_value)] if self.last_value != None else []
        if len(self.sub_topics) == 0:
            return topic_pair

        t_list = []
        for st in self.sub_topics:
            t_list = t_list + self.sub_topics[st].get_all_topic_values()
        return [(self.name + "/" + x[0], x[1]) for x in t_list] + topic_pair

    def get_topic_values(self, topic_path):
        if len(topic_path) == 1:
            return self.get_all_topic_values()

        for st in self.sub_topics:
            if st == topic_path[1]:
                return self.sub_topics[st].get_topic_values(topic_path[1:])
        return None

    def set_topic_value(self, value, topic_path):
        if len(topic_path) == 1:
            self.last_value = value
            return True
        else:
            if topic_path[1] not in self.sub_topics:
                self.sub_topics[topic_path[1]] = Topic(topic_path[1])
            self.sub_topics[topic_path[1]].set_topic_value(
                value, topic_path[1:])