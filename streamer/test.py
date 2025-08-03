import pygame
import cv2
import sys
import time

def play_video(filename, screen):
    print(f'\n[START] {filename} - {time.perf_counter_ns():.3f} sekundy')

    cap = cv2.VideoCapture(filename)
    if not cap.isOpened():
        print(f'Nie można otworzyć pliku: {filename}')
        return time.perf_counter_ns()

    fps = cap.get(cv2.CAP_PROP_FPS)
    clock = pygame.time.Clock()

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break

        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        frame = cv2.transpose(frame)
        frame = pygame.surfarray.make_surface(frame)

        screen.blit(pygame.transform.rotate(frame, -90), (0, 0))
        pygame.display.update()
        clock.tick(fps)

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                cap.release()
                pygame.quit()
                sys.exit()

    cap.release()
    end_time = time.perf_counter_ns()
    print(f'[END]   {filename} - {end_time:.3f} sekundy')
    return end_time

def main():
    pygame.init()

    probe = cv2.VideoCapture('video.mp4')
    if not probe.isOpened():
        print("Nie można otworzyć video.mp4")
        return

    width = int(probe.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(probe.get(cv2.CAP_PROP_FRAME_HEIGHT))
    probe.release()

    screen = pygame.display.set_mode((width, height))
    pygame.display.set_caption("Odtwarzacz Wideo")

    t1_end = play_video('video.mp4', screen)
    t2_start = time.perf_counter_ns()
    print(f'\n[PRZERWA] między video1 a video2: {t2_start - t1_end:.3f} sekundy\n')

    play_video('video.mp4', screen)

    pygame.quit()

if __name__ == '__main__':
    main()
