//import java.io.IOException;
//import java.io.InputStream;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Scanner;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.Semaphore;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//
//// <toplevel>
//import com.microsoft.cognitiveservices.speech.*;
//import com.microsoft.cognitiveservices.speech.audio.*;
//// </toplevel>
//
//public class SpeechToTextRef {
//
//    // Speech recognition from microphone.
//    public static void recognitionWithMicrophoneAsync() throws InterruptedException, ExecutionException {
//        // <recognitionWithMicrophone>
//        // Creates an instance of a speech config with specified
//        // subscription key and service region. Replace with your own subscription key
//        // and service region (e.g., "westus").
//        // The default language is "en-us".
//        SpeechConfig config = SpeechConfig.fromSubscription("YourSubscriptionKey", "YourServiceRegion");
//
//        // Creates a speech recognizer using microphone as audio input.
//        SpeechRecognizer recognizer = new SpeechRecognizer(config);
//        {
//            // Starts recognizing.
//            System.out.println("Say something...");
//
//            // Starts recognition. It returns when the first utterance has been recognized.
//            SpeechRecognitionResult result = recognizer.recognizeOnceAsync().get();
//
//            // Checks result.
//            if (result.getReason() == ResultReason.RecognizedSpeech) {
//                System.out.println("RECOGNIZED: Text=" + result.getText());
//            } else if (result.getReason() == ResultReason.NoMatch) {
//                System.out.println("NOMATCH: Speech could not be recognized.");
//            } else if (result.getReason() == ResultReason.Canceled) {
//                CancellationDetails cancellation = CancellationDetails.fromResult(result);
//                System.out.println("CANCELED: Reason=" + cancellation.getReason());
//
//                if (cancellation.getReason() == CancellationReason.Error) {
//                    System.out.println("CANCELED: ErrorCode=" + cancellation.getErrorCode());
//                    System.out.println("CANCELED: ErrorDetails=" + cancellation.getErrorDetails());
//                    System.out.println("CANCELED: Did you update the subscription info?");
//                }
//            }
//
//            result.close();
//        }
//
//        config.close();
//        recognizer.close();
//        // </recognitionWithMicrophone>
//    }
//
//    // Speech recognition in the specified spoken language.
//    public static void recognitionWithLanguageAsync() throws InterruptedException, ExecutionException {
//        // Creates an instance of a speech config with specified
//        // subscription key and service region. Replace with your own subscription key
//        // and service region (e.g., "westus").
//        SpeechConfig config = SpeechConfig.fromSubscription("YourSubscriptionKey", "YourServiceRegion");
//
//        // Creates a speech recognizer for the specified language, using microphone as audio input.
//        String lang = "de-de";
//        SpeechRecognizer recognizer = new SpeechRecognizer(config, lang);
//        {
//            // Starts recognizing.
//            System.out.println("Say something in " + lang + " ...");
//
//            // Starts recognition. It returns when the first utterance has been recognized.
//            SpeechRecognitionResult result = recognizer.recognizeOnceAsync().get();
//
//            // Checks result.
//            if (result.getReason() == ResultReason.RecognizedSpeech) {
//                System.out.println("RECOGNIZED: Text=" + result.getText());
//            } else if (result.getReason() == ResultReason.NoMatch) {
//                System.out.println("NOMATCH: Speech could not be recognized.");
//            } else if (result.getReason() == ResultReason.Canceled) {
//                CancellationDetails cancellation = CancellationDetails.fromResult(result);
//                System.out.println("CANCELED: Reason=" + cancellation.getReason());
//
//                if (cancellation.getReason() == CancellationReason.Error) {
//                    System.out.println("CANCELED: ErrorCode=" + cancellation.getErrorCode());
//                    System.out.println("CANCELED: ErrorDetails=" + cancellation.getErrorDetails());
//                    System.out.println("CANCELED: Did you update the subscription info?");
//                }
//            }
//            result.close();
//        }
//
//        config.close();
//        recognizer.close();
//    }
//
//    // Speech recognition using a customized model.
//    public static void recognitionUsingCustomizedModelAsync() throws InterruptedException, ExecutionException {
//        // <recognitionCustomized>
//        // Creates an instance of a speech config with specified
//        // subscription key and service region. Replace with your own subscription key
//        // and service region (e.g., "westus").
//        SpeechConfig config = SpeechConfig.fromSubscription("YourSubscriptionKey", "YourServiceRegion");
//        // Replace with the CRIS endpoint id of your customized model.
//        config.setEndpointId("YourEndpointId");
//
//        // Creates a speech recognizer using microphone as audio input.
//        SpeechRecognizer recognizer = new SpeechRecognizer(config);
//        {
//            // Starts recognizing.
//            System.out.println("Say something...");
//
//            // Starts recognition. It returns when the first utterance has been recognized.
//            SpeechRecognitionResult result = recognizer.recognizeOnceAsync().get();
//
//            // Checks result.
//            if (result.getReason() == ResultReason.RecognizedSpeech) {
//                System.out.println("RECOGNIZED: Text=" + result.getText());
//            } else if (result.getReason() == ResultReason.NoMatch) {
//                System.out.println("NOMATCH: Speech could not be recognized.");
//            } else if (result.getReason() == ResultReason.Canceled) {
//                CancellationDetails cancellation = CancellationDetails.fromResult(result);
//                System.out.println("CANCELED: Reason=" + cancellation.getReason());
//
//                if (cancellation.getReason() == CancellationReason.Error) {
//                    System.out.println("CANCELED: ErrorCode=" + cancellation.getErrorCode());
//                    System.out.println("CANCELED: ErrorDetails=" + cancellation.getErrorDetails());
//                    System.out.println("CANCELED: Did you update the subscription info?");
//                }
//            }
//
//            result.close();
//        }
//
//        config.close();
//        recognizer.close();
//        // </recognitionCustomized>
//    }
//
//    // Speech recognition with events from file
//    public static void continuousRecognitionWithFileAsync() throws InterruptedException, ExecutionException, IOException {
//        // <recognitionContinuousWithFile>
//        // Creates an instance of a speech config with specified
//        // subscription key and service region. Replace with your own subscription key
//        // and service region (e.g., "westus").
//        SpeechConfig config = SpeechConfig.fromSubscription("YourSubscriptionKey", "YourServiceRegion");
//
//        // Creates a speech recognizer using file as audio input.
//        // Replace with your own audio file name.
//        AudioConfig audioInput = AudioConfig.fromWavFileInput("YourAudioFile.wav");
//        SpeechRecognizer recognizer = new SpeechRecognizer(config, audioInput);
//        {
//            // Subscribes to events.
//            recognizer.recognizing.addEventListener((s, e) -> {
//                System.out.println("RECOGNIZING: Text=" + e.getResult().getText());
//            });
//
//            recognizer.recognized.addEventListener((s, e) -> {
//                if (e.getResult().getReason() == ResultReason.RecognizedSpeech) {
//                    System.out.println("RECOGNIZED: Text=" + e.getResult().getText());
//                } else if (e.getResult().getReason() == ResultReason.NoMatch) {
//                    System.out.println("NOMATCH: Speech could not be recognized.");
//                }
//            });
//
//            recognizer.canceled.addEventListener((s, e) -> {
//                System.out.println("CANCELED: Reason=" + e.getReason());
//
//                if (e.getReason() == CancellationReason.Error) {
//                    System.out.println("CANCELED: ErrorCode=" + e.getErrorCode());
//                    System.out.println("CANCELED: ErrorDetails=" + e.getErrorDetails());
//                    System.out.println("CANCELED: Did you update the subscription info?");
//                }
//            });
//
//            recognizer.sessionStarted.addEventListener((s, e) -> {
//                System.out.println("\n    Session started event.");
//            });
//
//            recognizer.sessionStopped.addEventListener((s, e) -> {
//                System.out.println("\n    Session stopped event.");
//            });
//
//            // Starts continuous recognition. Uses stopContinuousRecognitionAsync() to stop recognition.
//            System.out.println("Say something...");
//            recognizer.startContinuousRecognitionAsync().get();
//
//            System.out.println("Press any key to stop");
//            new Scanner(System.in).nextLine();
//
//            recognizer.stopContinuousRecognitionAsync().get();
//        }
//
//        config.close();
//        audioInput.close();
//        recognizer.close();
//        // </recognitionContinuousWithFile>
//    }
//
//    // The Source to stop recognition.
//    private static Semaphore stopRecognitionSemaphore;
//
//    // Speech recognition with audio stream
//    public static void recognitionWithAudioStreamAsync() throws InterruptedException, ExecutionException, FileNotFoundException {
//        stopRecognitionSemaphore = new Semaphore(0);
//
//        // Creates an instance of a speech config with specified
//        // subscription key and service region. Replace with your own subscription key
//        // and service region (e.g., "westus").
//        SpeechConfig config = SpeechConfig.fromSubscription("YourSubscriptionKey", "YourServiceRegion");
//
//        // Create an audio stream from a wav file.
//        // Replace with your own audio file name.
//        PullAudioInputStreamCallback callback = new WavStream(new FileInputStream("YourAudioFile.wav"));
//        AudioConfig audioInput = AudioConfig.fromStreamInput(callback);
//
//        // Creates a speech recognizer using audio stream input.
//        SpeechRecognizer recognizer = new SpeechRecognizer(config, audioInput);
//        {
//            // Subscribes to events.
//            recognizer.recognizing.addEventListener((s, e) -> {
//                System.out.println("RECOGNIZING: Text=" + e.getResult().getText());
//            });
//
//            recognizer.recognized.addEventListener((s, e) -> {
//                if (e.getResult().getReason() == ResultReason.RecognizedSpeech) {
//                    System.out.println("RECOGNIZED: Text=" + e.getResult().getText());
//                } else if (e.getResult().getReason() == ResultReason.NoMatch) {
//                    System.out.println("NOMATCH: Speech could not be recognized.");
//                }
//            });
//
//            recognizer.canceled.addEventListener((s, e) -> {
//                System.out.println("CANCELED: Reason=" + e.getReason());
//
//                if (e.getReason() == CancellationReason.Error) {
//                    System.out.println("CANCELED: ErrorCode=" + e.getErrorCode());
//                    System.out.println("CANCELED: ErrorDetails=" + e.getErrorDetails());
//                    System.out.println("CANCELED: Did you update the subscription info?");
//                }
//
//                stopRecognitionSemaphore.release();
//            });
//
//            recognizer.sessionStarted.addEventListener((s, e) -> {
//                System.out.println("\nSession started event.");
//            });
//
//            recognizer.sessionStopped.addEventListener((s, e) -> {
//                System.out.println("\nSession stopped event.");
//
//                // Stops translation when session stop is detected.
//                System.out.println("\nStop translation.");
//                stopRecognitionSemaphore.release();
//            });
//
//            // Starts continuous recognition. Uses stopContinuousRecognitionAsync() to stop recognition.
//            recognizer.startContinuousRecognitionAsync().get();
//
//            // Waits for completion.
//            stopRecognitionSemaphore.acquire();
//
//            // Stops recognition.
//            recognizer.stopContinuousRecognitionAsync().get();
//        }
//
//        config.close();
//        audioInput.close();
//        recognizer.close();
//    }
//
//    // Speech recognition with events from a push stream
//    // This sample takes and existing file and reads it by chunk into a local buffer and then pushes the
//    // buffer into an PushAudioStream for speech recognition.
//    public static void continuousRecognitionWithPushStream() throws InterruptedException, ExecutionException, IOException {
//        // Creates an instance of a speech config with specified
//        // subscription key and service region. Replace with your own subscription key
//        // and service region (e.g., "westus").
//        SpeechConfig config = SpeechConfig.fromSubscription("YourSubscriptionKey", "YourServiceRegion");
//
//        // Replace with your own audio file name.
//        // The input stream the sample will read from.
//        InputStream inputStream = new FileInputStream("YourAudioFile.wav");
//
//        // Create the push stream to push audio to.
//        PushAudioInputStream pushStream = AudioInputStream.createPushStream();
//
//        // Creates a speech recognizer using Push Stream as audio input.
//        AudioConfig audioInput = AudioConfig.fromStreamInput(pushStream);
//
//        SpeechRecognizer recognizer = new SpeechRecognizer(config, audioInput);
//        {
//            // Subscribes to events.
//            recognizer.recognizing.addEventListener((s, e) -> {
//                System.out.println("RECOGNIZING: Text=" + e.getResult().getText());
//            });
//
//            recognizer.recognized.addEventListener((s, e) -> {
//                if (e.getResult().getReason() == ResultReason.RecognizedSpeech) {
//                    System.out.println("RECOGNIZED: Text=" + e.getResult().getText());
//                } else if (e.getResult().getReason() == ResultReason.NoMatch) {
//                    System.out.println("NOMATCH: Speech could not be recognized.");
//                }
//            });
//
//            recognizer.canceled.addEventListener((s, e) -> {
//                System.out.println("CANCELED: Reason=" + e.getReason());
//
//                if (e.getReason() == CancellationReason.Error) {
//                    System.out.println("CANCELED: ErrorCode=" + e.getErrorCode());
//                    System.out.println("CANCELED: ErrorDetails=" + e.getErrorDetails());
//                    System.out.println("CANCELED: Did you update the subscription info?");
//                }
//            });
//
//            recognizer.sessionStarted.addEventListener((s, e) -> {
//                System.out.println("\n    Session started event.");
//            });
//
//            recognizer.sessionStopped.addEventListener((s, e) -> {
//                System.out.println("\n    Session stopped event.");
//            });
//
//            // Starts continuous recognition. Uses stopContinuousRecognitionAsync() to stop recognition.
//            System.out.println("Say something...");
//            recognizer.startContinuousRecognitionAsync().get();
//
//            // Arbitrary buffer size.
//            byte[] readBuffer = new byte[4096];
//
//            // Push audio read from the file into the PushStream.
//            // The audio can be pushed into the stream before, after, or during recognition
//            // and recognition will continue as data becomes available.
//            while (inputStream.read(readBuffer) != -1) {
//                pushStream.write(readBuffer);
//            }
//
//            pushStream.close();
//            inputStream.close();
//
//            System.out.println("Press any key to stop");
//            new Scanner(System.in).nextLine();
//
//            recognizer.stopContinuousRecognitionAsync().get();
//        }
//
//        config.close();
//        audioInput.close();
//        recognizer.close();
//    }
//
//    // Keyword-triggered speech recognition from microphone
//    public static void keywordTriggeredSpeechRecognitionWithMicrophone() throws InterruptedException, ExecutionException {
//        stopRecognitionSemaphore = new Semaphore(0);
//
//        // Creates an instance of a speech config with specified
//        // subscription key and service region. Replace with your own subscription key
//        // and service region (e.g., "westus").
//        SpeechConfig config = SpeechConfig.fromSubscription("YourSubscriptionKey", "YourServiceRegion");
//
//        // Creates a speech recognizer using microphone as audio input.
//        SpeechRecognizer recognizer = new SpeechRecognizer(config);
//        {
//            // Subscribes to events.
//            recognizer.recognizing.addEventListener((s, e) -> {
//                if (e.getResult().getReason() == ResultReason.RecognizingKeyword) {
//                    System.out.println("RECOGNIZING KEYWORD: Text=" + e.getResult().getText());
//                } else if (e.getResult().getReason() == ResultReason.RecognizingSpeech) {
//                    System.out.println("RECOGNIZING: Text=" + e.getResult().getText());
//                }
//            });
//
//            recognizer.recognized.addEventListener((s, e) -> {
//                if (e.getResult().getReason() == ResultReason.RecognizedSpeech) {
//                    System.out.println("RECOGNIZED: Text=" + e.getResult().getText());
//                } else if (e.getResult().getReason() == ResultReason.NoMatch) {
//                    System.out.println("NOMATCH: Speech could not be recognized.");
//                }
//            });
//
//            recognizer.canceled.addEventListener((s, e) -> {
//                System.out.println("CANCELED: Reason=" + e.getReason());
//
//                if (e.getReason() == CancellationReason.Error) {
//                    System.out.println("CANCELED: ErrorCode=" + e.getErrorCode());
//                    System.out.println("CANCELED: ErrorDetails=" + e.getErrorDetails());
//                    System.out.println("CANCELED: Did you update the subscription info?");
//                }
//            });
//
//            recognizer.sessionStarted.addEventListener((s, e) -> {
//                System.out.println("\n    Session started event.");
//            });
//
//            recognizer.sessionStopped.addEventListener((s, e) -> {
//                System.out.println("\n    Session stopped event.");
//
//                stopRecognitionSemaphore.release();
//            });
//
//            // Creates an instance of a keyword recognition model. Update this to
//            // point to the location of your keyword recognition model.
//            KeywordRecognitionModel model = KeywordRecognitionModel.fromFile("YourKeywordRecognitionModelFile.table");
//
//            // The phrase your keyword recognition model triggers on.
//            String keyword = "YourKeyword";
//
//            // Starts continuous recognition using the keyword model. Use
//            // stopKeywordRecognitionAsync() to stop recognition.
//            recognizer.startKeywordRecognitionAsync(model).get();
//
//            System.out.println("Say something starting with '" + keyword + "' followed by whatever you want...");
//
//            // Waits for a single successful keyword-triggered speech recognition (or error).
//            stopRecognitionSemaphore.acquire();
//
//            recognizer.stopKeywordRecognitionAsync().get();
//        }
//
//        config.close();
//        recognizer.close();
//    }
//
//    // Speech recognition with events from file
//    public static void continuousRecognitionWithFileWithPhraseListAsync() throws InterruptedException, ExecutionException, IOException {
//        // Creates an instance of a speech config with specified
//        // subscription key and service region. Replace with your own subscription key
//        // and service region (e.g., "westus").
//        SpeechConfig config = SpeechConfig.fromSubscription("YourSubscriptionKey", "YourServiceRegion");
//
//        // Creates a speech recognizer using file as audio input.
//        // Replace with your own audio file name.
//        // The audio file wreck-a-nice-beach.wav included with the C# sample contains ambigious audio.
//        AudioConfig audioInput = AudioConfig.fromWavFileInput("YourAudioFile.wav");
//        SpeechRecognizer recognizer = new SpeechRecognizer(config, audioInput);
//        {
//            // Create the recognizer.
//            PhraseListGrammar phraseList = PhraseListGrammar.fromRecognizer(recognizer);
//
//            // Add a phrase to assist in recognition.
//            phraseList.addPhrase("Wreck a nice beach");
//
//            // Subscribes to events.
//            recognizer.recognizing.addEventListener((s, e) -> {
//                System.out.println("RECOGNIZING: Text=" + e.getResult().getText());
//            });
//
//            recognizer.recognized.addEventListener((s, e) -> {
//                if (e.getResult().getReason() == ResultReason.RecognizedSpeech) {
//                    System.out.println("RECOGNIZED: Text=" + e.getResult().getText());
//                } else if (e.getResult().getReason() == ResultReason.NoMatch) {
//                    System.out.println("NOMATCH: Speech could not be recognized.");
//                }
//            });
//
//            recognizer.canceled.addEventListener((s, e) -> {
//                System.out.println("CANCELED: Reason=" + e.getReason());
//
//                if (e.getReason() == CancellationReason.Error) {
//                    System.out.println("CANCELED: ErrorCode=" + e.getErrorCode());
//                    System.out.println("CANCELED: ErrorDetails=" + e.getErrorDetails());
//                    System.out.println("CANCELED: Did you update the subscription info?");
//                }
//            });
//
//            recognizer.sessionStarted.addEventListener((s, e) -> {
//                System.out.println("\n    Session started event.");
//            });
//
//            recognizer.sessionStopped.addEventListener((s, e) -> {
//                System.out.println("\n    Session stopped event.");
//            });
//
//            // Starts continuous recognition. Uses stopContinuousRecognitionAsync() to stop recognition.
//            System.out.println("Say something...");
//            recognizer.startContinuousRecognitionAsync().get();
//
//            System.out.println("Press any key to stop");
//            new Scanner(System.in).nextLine();
//
//            recognizer.stopContinuousRecognitionAsync().get();
//        }
//
//        config.close();
//        audioInput.close();
//        recognizer.close();
//    }
//
//    // Speech recognition with events from file, also with source langauge auto detection
//    public static void continuousRecognitionWithFileAndSourceLanguageAutoDetection() throws InterruptedException, ExecutionException, IOException {
//        // Creates an instance of a speech config with specified
//        // subscription key and service region. Replace with your own subscription key
//        // and service region (e.g., "westus").
//        SpeechConfig config = SpeechConfig.fromSubscription("YourSubscriptionKey", "YourServiceRegion");
//
//        // Creates an instance of AutoDetectSourceLanguageConfig with the 2 source language candidates
//        // Currently this feature only supports 2 languages
//        // Replace the languages to be the language candidates for your speech. Please see https://docs.microsoft.com/azure/cognitive-services/speech-service/language-support for all supported langauges
//        AutoDetectSourceLanguageConfig autoDetectSourceLanguageConfig = AutoDetectSourceLanguageConfig.fromLanguages(Arrays.asList("en-US", "de-DE"));
//
//        // Replace with your own audio file name.
//        // The audio file wreck-a-nice-beach.wav included with the C# sample contains ambigious audio.
//        AudioConfig audioInput = AudioConfig.fromWavFileInput("YourAudioFile.wav");
//
//        // Creates a speech recognizer using file as audio input and the AutoDetectSourceLanguageConfig
//        SpeechRecognizer recognizer = new SpeechRecognizer(config, autoDetectSourceLanguageConfig, audioInput);
//        {
//            // Subscribes to events.
//            recognizer.recognizing.addEventListener((s, e) -> {
//                System.out.println("RECOGNIZING: Text=" + e.getResult().getText());
//                AutoDetectSourceLanguageResult autoDetectSourceLanguageResult = AutoDetectSourceLanguageResult.fromResult(e.getResult());
//                System.out.println("RECOGNIZING: Langauge=" + autoDetectSourceLanguageResult.getLanguage());
//            });
//
//            recognizer.recognized.addEventListener((s, e) -> {
//                AutoDetectSourceLanguageResult autoDetectSourceLanguageResult = AutoDetectSourceLanguageResult.fromResult(e.getResult());
//                String language = autoDetectSourceLanguageResult.getLanguage();
//                if (e.getResult().getReason() == ResultReason.RecognizedSpeech) {
//                    System.out.println("RECOGNIZED: Text=" + e.getResult().getText());
//
//                    System.out.println("RECOGNIZING: Langauge=" + language);
//                } else if (e.getResult().getReason() == ResultReason.NoMatch) {
//                    if (language == null || language.isEmpty()) {
//                        System.out.println("NOMATCH: Speech Language could not be detected.");
//                    } else {
//                        System.out.println("NOMATCH: Speech could not be recognized.");
//                    }
//                }
//            });
//
//            recognizer.canceled.addEventListener((s, e) -> {
//                System.out.println("CANCELED: Reason=" + e.getReason());
//
//                if (e.getReason() == CancellationReason.Error) {
//                    System.out.println("CANCELED: ErrorCode=" + e.getErrorCode());
//                    System.out.println("CANCELED: ErrorDetails=" + e.getErrorDetails());
//                    System.out.println("CANCELED: Did you update the subscription info?");
//                }
//            });
//
//            recognizer.sessionStarted.addEventListener((s, e) -> {
//                System.out.println("\n    Session started event.");
//            });
//
//            recognizer.sessionStopped.addEventListener((s, e) -> {
//                System.out.println("\n    Session stopped event.");
//            });
//
//            // Starts continuous recognition. Uses stopContinuousRecognitionAsync() to stop recognition.
//            System.out.println("Say something...");
//            recognizer.startContinuousRecognitionAsync().get();
//
//            System.out.println("Press any key to stop");
//            new Scanner(System.in).nextLine();
//
//            recognizer.stopContinuousRecognitionAsync().get();
//        }
//
//        config.close();
//        autoDetectSourceLanguageConfig.close();
//        audioInput.close();
//        recognizer.close();
//    }
//
//    // Speech recognition with events from file, also with source langauge auto detection and using customized model
//    public static void continuousRecognitionWithSourceLanguageAutoDetectionAndCustomizedModel() throws InterruptedException, ExecutionException, IOException {
//        // Creates an instance of a speech config with specified
//        // subscription key and service region. Replace with your own subscription key
//        // and service region (e.g., "westus").
//        SpeechConfig config = SpeechConfig.fromSubscription("YourSubscriptionKey", "YourServiceRegion");
//
//        List<SourceLanguageConfig> sourceLanguageConfigs = new ArrayList<SourceLanguageConfig>();
//        // The endpoint id is optional, if not specified,  the service will use the default model for en-US
//        // Replace the language with your source language candidate. Please see https://docs.microsoft.com/azure/cognitive-services/speech-service/language-support for all supported langauges
//        sourceLanguageConfigs.add(SourceLanguageConfig.fromLanguage("en-US"));
//        // Replace the id with the CRIS endpoint id of your customized model. If the speech is in fr-FR, the service will use the corresponding customized model for speech recognition
//        sourceLanguageConfigs.add(SourceLanguageConfig.fromLanguage("fr-FR", "The Endpoint Id for custom model of fr-FR"));
//
//        // Creates an instance of AutoDetectSourceLanguageConfig with the 2 source language configurations
//        // Currently this feature only supports 2 languages
//        AutoDetectSourceLanguageConfig autoDetectSourceLanguageConfig = AutoDetectSourceLanguageConfig.fromSourceLanguageConfigs(sourceLanguageConfigs);
//
//        // Replace with your own audio file name.
//        // The audio file wreck-a-nice-beach.wav included with the C# sample contains ambigious audio.
//        AudioConfig audioInput = AudioConfig.fromWavFileInput("YourAudioFile.wav");
//
//        // Creates a speech recognizer using file as audio input and the AutoDetectSourceLanguageConfig
//        SpeechRecognizer recognizer = new SpeechRecognizer(config, autoDetectSourceLanguageConfig, audioInput);
//        {
//            // Subscribes to events.
//            recognizer.recognizing.addEventListener((s, e) -> {
//                System.out.println("RECOGNIZING: Text=" + e.getResult().getText());
//                AutoDetectSourceLanguageResult autoDetectSourceLanguageResult = AutoDetectSourceLanguageResult.fromResult(e.getResult());
//                System.out.println("RECOGNIZING: Langauge=" + autoDetectSourceLanguageResult.getLanguage());
//            });
//
//            recognizer.recognized.addEventListener((s, e) -> {
//                AutoDetectSourceLanguageResult autoDetectSourceLanguageResult = AutoDetectSourceLanguageResult.fromResult(e.getResult());
//                String language = autoDetectSourceLanguageResult.getLanguage();
//                if (e.getResult().getReason() == ResultReason.RecognizedSpeech) {
//                    System.out.println("RECOGNIZED: Text=" + e.getResult().getText());
//
//                    System.out.println("RECOGNIZING: Langauge=" + language);
//                } else if (e.getResult().getReason() == ResultReason.NoMatch) {
//                    if (language == null || language.isEmpty()) {
//                        System.out.println("NOMATCH: Speech Language could not be detected.");
//                    } else {
//                        System.out.println("NOMATCH: Speech could not be recognized.");
//                    }
//                }
//            });
//
//            recognizer.canceled.addEventListener((s, e) -> {
//                System.out.println("CANCELED: Reason=" + e.getReason());
//
//                if (e.getReason() == CancellationReason.Error) {
//                    System.out.println("CANCELED: ErrorCode=" + e.getErrorCode());
//                    System.out.println("CANCELED: ErrorDetails=" + e.getErrorDetails());
//                    System.out.println("CANCELED: Did you update the subscription info?");
//                }
//            });
//
//            recognizer.sessionStarted.addEventListener((s, e) -> {
//                System.out.println("\n    Session started event.");
//            });
//
//            recognizer.sessionStopped.addEventListener((s, e) -> {
//                System.out.println("\n    Session stopped event.");
//            });
//
//            // Starts continuous recognition. Uses stopContinuousRecognitionAsync() to stop recognition.
//            System.out.println("Say something...");
//            recognizer.startContinuousRecognitionAsync().get();
//
//            System.out.println("Press any key to stop");
//            new Scanner(System.in).nextLine();
//
//            recognizer.stopContinuousRecognitionAsync().get();
//        }
//
//        config.close();
//        for (SourceLanguageConfig sourceLanguageConfig : sourceLanguageConfigs) {
//            sourceLanguageConfig.close();
//        }
//        autoDetectSourceLanguageConfig.close();
//        audioInput.close();
//        recognizer.close();
//    }
//}
