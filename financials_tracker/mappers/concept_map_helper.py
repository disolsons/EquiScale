import yaml

class ConceptMapHelper:
    CONFIG_MAP_PATH = "financials_tracker/mappers/config/concept_map.yaml"

    def __init__(self, path=CONFIG_MAP_PATH):
        self.concept_map = self._load_concept_map(path)
        self.tag_lookup = self._build_tag_lookup()

    def _load_concept_map(self, path):
        with open(path, "r") as f:
            return yaml.safe_load(f)        
        
    
    def _build_tag_lookup(self):
        """
        Flattens the concept map to be able to quickly lookup specific tags.
        """
        lookup = {}

        for statement, concepts in self.concept_map.items():
            for concept, tags in concepts.items():
                for t in tags:
                    lookup[t] = {
                        "concept": concept,
                        "statement": statement
                    }

        return lookup

    def get_concept_from_tag(self, tag):
        return self.tag_lookup.get(tag)

    def get_tags_for_concept(self, type_of_statement, concept):
        return self.concept_map.get(type_of_statement, {}).get(concept, [])
    
    def is_known_tag(self, tag):
        return tag in self.tag_lookup
    
