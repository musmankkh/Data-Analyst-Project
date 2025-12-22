import cv2
import numpy as np
from tensorflow import keras
from collections import deque
import time

class SignLanguageDetectorCV:
    def __init__(self, model_path):
        """Initialize the sign language detector using OpenCV only"""
        # Load model
        print("Loading model...")
        self.model = keras.models.load_model(model_path)
        print("✓ Model loaded successfully!")
        
        # Class labels
        self.class_labels = {
            0: 'A', 1: 'B', 2: 'C', 3: 'D', 4: 'E', 5: 'F', 6: 'G', 7: 'H', 8: 'I',
            9: 'K', 10: 'L', 11: 'M', 12: 'N', 13: 'O', 14: 'P', 15: 'Q', 16: 'R',
            17: 'S', 18: 'T', 19: 'U', 20: 'V', 21: 'W', 22: 'X', 23: 'Y'
        }
        
        # Prediction smoothing
        self.prediction_buffer = deque(maxlen=10)
        self.last_prediction = None
        self.last_confidence = 0
        
        # FPS calculation
        self.fps_buffer = deque(maxlen=30)
        self.last_time = time.time()
        
        # Background subtractor for hand detection
        self.bg_subtractor = cv2.createBackgroundSubtractorMOG2(
            history=500, varThreshold=16, detectShadows=False
        )
        
        # Detection mode
        self.use_skin_detection = True
        self.calibration_frames = 0
        self.max_calibration = 30
    
    def detect_skin(self, frame):
        """Detect skin color regions"""
        # Convert to HSV
        hsv = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)
        
        # Define skin color range in HSV
        lower_skin = np.array([0, 20, 70], dtype=np.uint8)
        upper_skin = np.array([20, 255, 255], dtype=np.uint8)
        
        # Create mask
        mask = cv2.inRange(hsv, lower_skin, upper_skin)
        
        # Apply morphological operations
        kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5))
        mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel)
        mask = cv2.morphologyEx(mask, cv2.MORPH_OPEN, kernel)
        
        # Blur to remove noise
        mask = cv2.GaussianBlur(mask, (5, 5), 0)
        
        return mask
    
    def find_largest_contour(self, mask):
        """Find the largest contour (assumed to be hand)"""
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        if not contours:
            return None, None
        
        # Find largest contour
        largest_contour = max(contours, key=cv2.contourArea)
        area = cv2.contourArea(largest_contour)
        
        # Filter small contours
        if area < 3000:
            return None, None
        
        # Get bounding box
        x, y, w, h = cv2.boundingRect(largest_contour)
        
        return largest_contour, (x, y, w, h)
    
    def extract_hand_roi(self, frame, bbox):
        """Extract hand region of interest"""
        x, y, w, h = bbox
        h_frame, w_frame, _ = frame.shape
        
        # Add padding
        padding = 40
        x_min = max(0, x - padding)
        x_max = min(w_frame, x + w + padding)
        y_min = max(0, y - padding)
        y_max = min(h_frame, y + h + padding)
        
        # Extract ROI
        roi = frame[y_min:y_max, x_min:x_max]
        
        if roi.size == 0:
            return None, None
        
        return roi, (x_min, y_min, x_max, y_max)
    
    def preprocess_roi(self, roi):
        """Preprocess ROI for model prediction"""
        # Convert to grayscale
        gray = cv2.cvtColor(roi, cv2.COLOR_BGR2GRAY)
        
        # Resize to 28x28
        resized = cv2.resize(gray, (28, 28))
        
        # Normalize
        normalized = resized / 255.0
        
        # Reshape for model
        processed = normalized.reshape(1, 28, 28, 1)
        
        return processed
    
    def get_smoothed_prediction(self, prediction):
        """Smooth predictions using buffer"""
        predicted_class = np.argmax(prediction)
        confidence = np.max(prediction) * 100
        
        # Add to buffer
        self.prediction_buffer.append((predicted_class, confidence))
        
        # Get most common prediction from buffer
        if len(self.prediction_buffer) >= 5:
            classes = [p[0] for p in self.prediction_buffer]
            confidences = [p[1] for p in self.prediction_buffer]
            
            # Most frequent class
            unique, counts = np.unique(classes, return_counts=True)
            most_common_class = unique[np.argmax(counts)]
            avg_confidence = np.mean([c for cls, c in self.prediction_buffer if cls == most_common_class])
            
            return most_common_class, avg_confidence
        
        return predicted_class, confidence
    
    def draw_ui(self, frame, hand_bbox=None, prediction=None, confidence=None, fps=None, calibrating=False):
        """Draw UI elements on frame"""
        h, w, c = frame.shape
        
        # Draw semi-transparent overlay for info panel
        overlay = frame.copy()
        panel_height = 180 if calibrating else 150
        cv2.rectangle(overlay, (10, 10), (450, panel_height), (0, 0, 0), -1)
        frame = cv2.addWeighted(overlay, 0.6, frame, 0.4, 0)
        
        # Title
        cv2.putText(frame, "Sign Language Detector (OpenCV)", (20, 40),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 2)
        
        # Calibration status
        if calibrating:
            progress = int((self.calibration_frames / self.max_calibration) * 100)
            cv2.putText(frame, f"Calibrating... {progress}%", (20, 70),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 255), 2)
            cv2.putText(frame, "Keep hand in view", (20, 95),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 255), 1)
        
        # FPS
        if fps:
            y_pos = 120 if calibrating else 70
            cv2.putText(frame, f"FPS: {fps:.1f}", (20, y_pos),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)
        
        # Prediction
        if prediction and confidence and not calibrating:
            y_pos = 100
            color = (0, 255, 0) if confidence > 70 else (0, 255, 255) if confidence > 50 else (0, 165, 255)
            cv2.putText(frame, f"Letter: {prediction}", (20, y_pos),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.7, color, 2)
            cv2.putText(frame, f"Confidence: {confidence:.1f}%", (20, y_pos + 30),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.6, color, 2)
        elif not calibrating:
            cv2.putText(frame, "No hand detected", (20, 100),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 0, 255), 2)
        
        # Draw hand bounding box
        if hand_bbox:
            x_min, y_min, x_max, y_max = hand_bbox
            cv2.rectangle(frame, (x_min, y_min), (x_max, y_max), (0, 255, 0), 2)
            
            # Draw ROI indicator
            center_x = (x_min + x_max) // 2
            center_y = (y_min + y_max) // 2
            cv2.circle(frame, (center_x, center_y), 5, (0, 255, 0), -1)
        
        # Detection zone guide (center of screen)
        zone_size = 300
        zone_x = w // 2 - zone_size // 2
        zone_y = h // 2 - zone_size // 2
        cv2.rectangle(frame, (zone_x, zone_y), (zone_x + zone_size, zone_y + zone_size),
                     (100, 100, 100), 2)
        cv2.putText(frame, "Place hand here", (zone_x + 60, zone_y - 10),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.5, (100, 100, 100), 1)
        
        # Instructions
        cv2.putText(frame, "q: Quit | r: Reset | c: Recalibrate", (10, h - 20),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 1)
        
        return frame
    
    def calculate_fps(self):
        """Calculate FPS"""
        current_time = time.time()
        fps = 1 / (current_time - self.last_time)
        self.last_time = current_time
        self.fps_buffer.append(fps)
        return np.mean(self.fps_buffer)
    
    def run(self):
        """Run real-time detection"""
        cap = cv2.VideoCapture(0)
        
        if not cap.isOpened():
            print("✗ Error: Could not open webcam")
            return
        
        # Set camera properties
        cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)
        
        print("\n" + "="*60)
        print("Real-Time Sign Language Detection Active (OpenCV Mode)")
        print("="*60)
        print("Controls:")
        print("  'q' - Quit")
        print("  'r' - Reset predictions")
        print("  'c' - Recalibrate background")
        print("\nTips:")
        print("  - Place hand in center detection zone")
        print("  - Use good lighting")
        print("  - Keep background simple")
        print("="*60 + "\n")
        
        try:
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                
                # Flip frame horizontally for selfie view
                frame = cv2.flip(frame, 1)
                
                # Calibration phase
                calibrating = self.calibration_frames < self.max_calibration
                if calibrating:
                    self.bg_subtractor.apply(frame)
                    self.calibration_frames += 1
                
                prediction = None
                confidence = None
                hand_bbox = None
                
                if not calibrating:
                    # Detect skin regions
                    skin_mask = self.detect_skin(frame)
                    
                    # Find hand contour
                    contour, bbox = self.find_largest_contour(skin_mask)
                    
                    if bbox is not None:
                        # Extract hand ROI
                        roi, hand_bbox = self.extract_hand_roi(frame, bbox)
                        
                        if roi is not None:
                            # Preprocess and predict
                            processed_roi = self.preprocess_roi(roi)
                            pred = self.model.predict(processed_roi, verbose=0)
                            
                            # Get smoothed prediction
                            pred_class, conf = self.get_smoothed_prediction(pred)
                            prediction = self.class_labels.get(pred_class, "Unknown")
                            confidence = conf
                            
                            self.last_prediction = prediction
                            self.last_confidence = confidence
                    else:
                        # Use last prediction if available
                        if self.last_prediction:
                            prediction = self.last_prediction
                            confidence = self.last_confidence
                
                # Calculate FPS
                fps = self.calculate_fps()
                
                # Draw UI
                frame = self.draw_ui(frame, hand_bbox, prediction, confidence, fps, calibrating)
                
                # Display
                cv2.imshow('Sign Language Detection', frame)
                
                # Handle key presses
                key = cv2.waitKey(1) & 0xFF
                if key == ord('q'):
                    break
                elif key == ord('r'):
                    self.prediction_buffer.clear()
                    self.last_prediction = None
                    self.last_confidence = 0
                    print("✓ Predictions reset!")
                elif key == ord('c'):
                    self.calibration_frames = 0
                    self.bg_subtractor = cv2.createBackgroundSubtractorMOG2(
                        history=500, varThreshold=16, detectShadows=False
                    )
                    print("✓ Recalibrating background...")
        
        finally:
            cap.release()
            cv2.destroyAllWindows()
            print("\n✓ Detection stopped. Goodbye!")

def main():
    """Main function"""
    model_path = r"D:\Intern\Sign Language CV\Dataset\sign_language_model.h5"
    
    try:
        # Initialize detector
        detector = SignLanguageDetectorCV(model_path)
        
        # Run detection
        detector.run()
        
    except FileNotFoundError:
        print(f"✗ Error: Model file not found at {model_path}")
        print("Please check the path and try again.")
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()