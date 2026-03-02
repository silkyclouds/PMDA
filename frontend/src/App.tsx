import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route, Navigate, useParams } from "react-router-dom";
import { ThemeProvider } from "next-themes";
import Scan from "./pages/Scan";
import Statistics from "./pages/Statistics";
import ListeningStatsPage from "./pages/ListeningStats";
import LibraryStatsPage from "./pages/LibraryStats";
import LibraryLayout from "./pages/LibraryLayout";
import LibraryHome from "./pages/LibraryHome";
import LibraryHomeFeed from "./pages/LibraryHomeFeed";
import LibraryAlbums from "./pages/LibraryAlbums";
import LibraryArtists from "./pages/LibraryArtists";
import LibraryGenres from "./pages/LibraryGenres";
import LibraryLabels from "./pages/LibraryLabels";
import ArtistPage from "./pages/ArtistPage";
import LabelPage from "./pages/LabelPage";
import GenrePage from "./pages/GenrePage";
import AlbumPage from "./pages/AlbumPage";
import Playlists from "./pages/Playlists";
import PlaylistDetail from "./pages/PlaylistDetail";
import TagFixer from "./pages/TagFixer";
import Settings from "./pages/Settings";
import NotFound from "./pages/NotFound";
import Tools from "./pages/Tools";
import Unduper from "./pages/Unduper";
import { BrokenAlbumsList } from "./components/broken-albums/BrokenAlbumsList";
import { PlaybackProvider, usePlayback } from "./contexts/PlaybackContext";
import { AudioPlayer } from "./components/library/AudioPlayer";
import { ScanFinishedInvalidator } from "./components/ScanFinishedInvalidator";
import { AssistantDock } from "./components/assistant/AssistantDock";
import { AppLayout } from "./components/layout/AppLayout";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      retryDelay: 1200,
      refetchOnWindowFocus: false,
    },
  },
});

function PlaylistLegacyRedirect() {
  const { playlistId } = useParams();
  if (!playlistId) return <Navigate to="/library/playlists" replace />;
  return <Navigate to={`/library/playlists/${playlistId}`} replace />;
}

function AppRoutesWithPlayer() {
  const { session, setCurrentTrack, closePlayer, recommendationSessionId } = usePlayback();
  return (
    <>
      <ScanFinishedInvalidator />
      <Routes>
        <Route element={<AppLayout />}>
          <Route path="/" element={<Navigate to="/library" replace />} />
          <Route path="/scan" element={<Scan />} />
          <Route path="/unduper" element={<Navigate to="/tools/duplicates" replace />} />
          <Route path="/history" element={<Navigate to="/tools" replace />} />
          <Route path="/statistics" element={<Statistics />} />
          <Route path="/statistics/listening" element={<ListeningStatsPage />} />
          <Route path="/statistics/library" element={<LibraryStatsPage />} />
          <Route path="/tools" element={<Tools />} />
          <Route path="/tools/duplicates" element={<Unduper />} />
          <Route path="/library" element={<LibraryLayout />}>
            <Route index element={<LibraryHome />} />
            <Route path="home" element={<LibraryHome />} />
            <Route path="home/feed/:section" element={<LibraryHomeFeed />} />
            <Route path="artists" element={<LibraryArtists />} />
            <Route path="albums" element={<LibraryAlbums />} />
            <Route path="genres" element={<LibraryGenres />} />
            <Route path="labels" element={<LibraryLabels />} />
            <Route path="artist/:artistId" element={<ArtistPage />} />
            <Route path="label/:label" element={<LabelPage />} />
            <Route path="genre/:genre" element={<GenrePage />} />
            <Route path="album/:albumId" element={<AlbumPage />} />
            <Route path="playlists" element={<Playlists />} />
            <Route path="playlists/:playlistId" element={<PlaylistDetail />} />
            <Route path="browser" element={<Navigate to="/library" replace />} />
          </Route>
          {/* Legacy aliases (avoid 404 on direct URL access) */}
          <Route path="/playlists" element={<Navigate to="/library/playlists" replace />} />
          <Route path="/playlists/:playlistId" element={<PlaylistLegacyRedirect />} />
          <Route path="/tag-fixer" element={<TagFixer />} />
          <Route path="/broken-albums" element={<BrokenAlbumsList />} />
          <Route path="/settings" element={<Settings />} />
          {/* ADD ALL CUSTOM ROUTES ABOVE THE CATCH-ALL "*" ROUTE */}
          <Route path="*" element={<NotFound />} />
        </Route>
      </Routes>
      <AssistantDock bottomOffsetPx={session ? 128 : 16} />
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
