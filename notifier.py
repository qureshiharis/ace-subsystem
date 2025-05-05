import os
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

USE_GPIO = os.getenv("USE_GPIO", "false").lower() == "true"
LED_PIN = int(os.getenv("LED_PIN", 18))
BUZZER_PIN = int(os.getenv("BUZZER_PIN", 23))

GPIO_AVAILABLE = False

if USE_GPIO:
    try:
        import RPi.GPIO as GPIO
        GPIO.setmode(GPIO.BCM)
        GPIO.setwarnings(False)
        GPIO.setup(LED_PIN, GPIO.OUT)
        GPIO.setup(BUZZER_PIN, GPIO.OUT)
        GPIO_AVAILABLE = True
        logger.info("GPIO setup complete.")
    except (ImportError, RuntimeError) as e:
        logger.warning(f"Not using GPIO: {e}")
        GPIO_AVAILABLE = False
else:
    logger.info("GPIO usage is disabled via USE_GPIO=false")

def alert():
    if GPIO_AVAILABLE:
        logger.info("Activating LED and Buzzer")
        GPIO.output(LED_PIN, GPIO.HIGH)
        GPIO.output(BUZZER_PIN, GPIO.HIGH)
        time.sleep(1)
        GPIO.output(LED_PIN, GPIO.LOW)
        GPIO.output(BUZZER_PIN, GPIO.LOW)
    else:
        logger.info("Mock alert triggered (no GPIO available)")