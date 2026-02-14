import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { ThemeProvider } from "next-themes";
import Scan from "./pages/Scan";
import Statistics from "./pages/Statistics";
import LibraryBrowser from "./pages/LibraryBrowser";
import ArtistPage from "./pages/ArtistPage";
import TagFixer from "./pages/TagFixer";
import Settings from "./pages/Settings";
import NotFound from "./pages/NotFound";
import Tools from "./pages/Tools";
import { BrokenAlbumsList } from "./components/broken-albums/BrokenAlbumsList";
import { PlaybackProvider, usePlayback } from "./contexts/PlaybackContext";
import { AudioPlayer } from "./components/library/AudioPlayer";
import { ScanFinishedInvalidator } from "./components/ScanFinishedInvalidator";

const queryClient = new QueryClient();

function AppRoutesWithPlayer() {
  const { session, setCurrentTrack, closePlayer, recommendationSessionId } = usePlayback();
  return (
    <>
      <ScanFinishedInvalidator />
      <Routes>
        <Route path="/" element={<Scan />} />
        <Route path="/unduper" element={<Navigate to="/tools" replace />} />
        <Route path="/history" element={<Navigate to="/tools" replace />} />
        <Route path="/statistics" element={<Statistics />} />
        <Route path="/tools" element={<Tools />} />
        <Route path="/library" element={<LibraryBrowser />} />
        <Route path="/library/artist/:artistId" element={<ArtistPage />} />
        <Route path="/tag-fixer" element={<TagFixer />} />
        <Route path="/broken-albums" element={<BrokenAlbumsList />} />
        <Route path="/settings" element={<Settings />} />
        {/* ADD ALL CUSTOM ROUTES ABOVE THE CATCH-ALL "*" ROUTE */}
        <Route path="*" element={<NotFound />} />
      </Routes>
      {session && (
        <AudioPlayer
          albumId={session.albumId}
          albumTitle={session.albumTitle}
          albumThumb={session.albumThumb}
          tracks={session.tracks}
          currentTrack={session.currentTrack}
          recommendationSessionId={recommendationSessionId}
          onTrackSelect={setCurrentTrack}
          onClose={closePlayer}
        />
      )}
    </>
  );
}

const App = () => (
  <ThemeProvider attribute="class" defaultTheme="system" enableSystem disableTransitionOnChange>
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <Toaster />
        <Sonner />
        <BrowserRouter>
          <PlaybackProvider>
            <AppRoutesWithPlayer />
          </PlaybackProvider>
        </BrowserRouter>
      </TooltipProvider>
    </QueryClientProvider>
  </ThemeProvider>
);

export default App;
