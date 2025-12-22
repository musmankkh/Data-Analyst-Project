import cv2
import numpy as np
import matplotlib.pyplot as plt
from tensorflow import keras

def get_letter(result):
    """Convert numeric prediction to letter"""
    class_labels = {
        0: 'A', 1: 'B', 2: 'C', 3: 'D', 4: 'E', 5: 'F', 6: 'G', 7: 'H', 8: 'I',
        9: 'K', 10: 'L', 11: 'M', 12: 'N', 13: 'O', 14: 'P', 15: 'Q', 16: 'R',
        17: 'S', 18: 'T', 19: 'U', 20: 'V', 21: 'W', 22: 'X', 23: 'Y'
    }
    try:
        return class_labels[int(result)]
    except:
        return "Error"

def preprocess_custom_image(image_path):
    """Preprocess custom image for prediction"""
    # Read image
    img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
    
    if img is None:
        print(f"Error: Could not read image from {image_path}")
        return None, None
    
    # Resize to 28x28
    img_resized = cv2.resize(img, (28, 28))
    
    # Normalize
    img_normalized = img_resized / 255.0
    
    # Reshape for model
    img_final = img_normalized.reshape(1, 28, 28, 1)
    
    return img_final, img_resized

def predict_sign(image_path, model, show_image=True):
    """Predict sign language from custom image"""
    # Preprocess
    processed_img, original_resized = preprocess_custom_image(image_path)
    
    if processed_img is None:
        return None, None
    
    # Predict
    prediction = model.predict(processed_img, verbose=0)
    predicted_class = np.argmax(prediction)
    confidence = np.max(prediction) * 100
    predicted_letter = get_letter(predicted_class)
    
    # Display results
    if show_image:
        plt.figure(figsize=(10, 4))
        
        # Original resized image
        plt.subplot(1, 2, 1)
        plt.imshow(original_resized, cmap='gray')
        plt.title('Input Image (28x28)', fontsize=12, fontweight='bold')
        plt.axis('off')
        
        # Prediction probabilities
        plt.subplot(1, 2, 2)
        top_5_idx = np.argsort(prediction[0])[-5:][::-1]
        top_5_prob = prediction[0][top_5_idx] * 100
        top_5_letters = [get_letter(i) for i in top_5_idx]
        
        colors = ['green' if i == 0 else 'skyblue' for i in range(5)]
        plt.barh(top_5_letters, top_5_prob, color=colors)
        plt.xlabel('Confidence (%)', fontsize=12)
        plt.title('Top 5 Predictions', fontsize=12, fontweight='bold')
        plt.xlim(0, 100)
        
        for i, (letter, prob) in enumerate(zip(top_5_letters, top_5_prob)):
            plt.text(prob + 1, i, f'{prob:.1f}%', va='center')
        
        plt.tight_layout()
        plt.show()
    
    print(f"\n{'=' * 50}")
    print(f"PREDICTION RESULT")
    print(f"{'=' * 50}")
    print(f"Predicted Letter: {predicted_letter}")
    print(f"Confidence: {confidence:.2f}%")
    print(f"{'=' * 50}\n")
    
    return predicted_letter, confidence

def main():
    """Main function to run predictions"""
    # Load the model
    print("Loading model...")
    model_path = r"D:\Intern\Sign Language CV\Dataset\sign_language_model.h5"
    
    try:
        model = keras.models.load_model(model_path)
        print("✓ Model loaded successfully!")
    except Exception as e:
        print(f"✗ Error loading model: {e}")
        return
    
    # Test single or multiple images
    while True:
        print("\n" + "="*50)
        print("OPTIONS:")
        print("1. Test single image")
        print("2. Test multiple images")
        print("3. Exit")
        print("="*50)
        
        choice = input("Enter your choice (1-3): ").strip()
        
        if choice == '1':
            image_path = input("\nEnter path to test image: ").strip()
            predict_sign(image_path, model, show_image=True)
            
        elif choice == '2':
            num_images = int(input("\nHow many images to test? "))
            for i in range(num_images):
                image_path = input(f"\nEnter path to image {i+1}: ").strip()
                predict_sign(image_path, model, show_image=True)
                
        elif choice == '3':
            print("\nExiting... Goodbye!")
            break
        else:
            print("\nInvalid choice. Please try again.")

if __name__ == "__main__":
    main()