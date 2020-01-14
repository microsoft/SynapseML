import json

extractKeyPhrases = {"documents": [
		{ "language": "en", "id": "1", 
			"text": "Hello world. This is some input text that I love."}, 
		{ "language": "fr", "id": "2", 
			"text": "Bonjour tout le monde"}, 
		{ "language": "es", "id": "3", 
			"text": "La carretera estaba atascada. Había mucho tráfico el día de ayer." } 
	]}

detectLanguage = {
  "documents": [
    {
      "id": "1",
      "text": "Hello world"
    },
    {
      "id": "2",
      "text": "Bonjour tout le monde"
    },
    {
      "id": "3",
      "text": "La carretera estaba atascada. Había mucho tráfico el día de ayer."
    },
    {
      "id": "4",
      "text": ":) :( :D"
    }
  ]
}

sentimentAnalysis = {
  "documents": [
    {
      "language": "en",
      "id": "1",
      "text": "Hello world. This is some input text that I love."
    },
    {
      "language": "fr",
      "id": "2",
      "text": "Bonjour tout le monde"
    },
    {
      "language": "es",
      "id": "3",
      "text": "La carretera estaba atascada. Había mucho tráfico el día de ayer."
    }
  ]
}