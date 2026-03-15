import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route, Navigate, useLocation, useParams } from "react-router-dom";
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
import LikedPage from "./pages/LikedPage";
import RecommendationsPage from "./pages/RecommendationsPage";
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
import LoginPage from "./pages/Login";
import BootstrapAdminPage from "./pages/BootstrapAdmin";
import AdminUsersPage from "./pages/AdminUsers";
import { BrokenAlbumsList } from "./components/broken-albums/BrokenAlbumsList";
import { PlaybackProvider, usePlayback } from "./contexts/PlaybackContext";
import { AuthProvider, useAuth } from "./contexts/AuthContext";
import { AudioPlayer } from "./components/library/AudioPlayer";
import { ScanFinishedInvalidator } from "./components/ScanFinishedInvalidator";
import { AssistantDock } from "./components/assistant/AssistantDock";
import { AppLayout } from "./components/layout/AppLayout";
import { useTaskEvents } from "./hooks/useTaskEvents";

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

function LoginRedirect() {
  const location = useLocation();
  const nextRaw = `${location.pathname || ""}${location.search || ""}${location.hash || ""}`;
  const nextPath = nextRaw.startsWith("/") ? nextRaw : "/library";
  return <Navigate to={`/auth/login?next=${encodeURIComponent(nextPath)}`} replace />;
}

function AuthLoadingScreen() {
  return (
    <div className="flex min-h-screen items-center justify-center bg-background p-6">
      <p className="text-sm text-muted-foreground">Loading authentication…</p>
    </div>
  );
}

function AppRoutesWithPlayer() {
  const { session, setCurrentTrack, closePlayer, recommendationSessionId } = usePlayback();
  const auth = useAuth();
  useTaskEvents({ enabled: Boolean(auth.user?.is_admin), pollIntervalMs: 3000 });

  if (auth.isLoading) {
    return <AuthLoadingScreen />;
  }

  if (auth.bootstrapRequired) {
    return (
      <Routes>
        <Route path="/auth/bootstrap" element={<BootstrapAdminPage />} />
        <Route path="*" element={<Navigate to="/auth/bootstrap" replace />} />
      </Routes>
    );
  }

  if (!auth.user) {
    return (
      <Routes>
        <Route path="/auth/login" element={<LoginPage />} />
        <Route path="*" element={<LoginRedirect />} />
      </Routes>
    );
  }

  const adminOnly = (element: JSX.Element) => (auth.isAdmin ? element : <Navigate to="/library" replace />);
  const statsAllowed = auth.isAdmin || auth.canViewStatistics;
  const statsOnly = (element: JSX.Element) => (statsAllowed ? element : <Navigate to="/library" replace />);

  return (
    <>
      {auth.isAdmin ? <ScanFinishedInvalidator /> : null}
      <Routes>
        <Route element={<AppLayout />}>
          <Route path="/" element={<Navigate to="/library" replace />} />
          <Route path="/scan" element={adminOnly(<Scan />)} />
          <Route path="/unduper" element={<Navigate to={auth.isAdmin ? "/tools/duplicates" : "/library"} replace />} />
          <Route path="/history" element={<Navigate to={auth.isAdmin ? "/tools" : "/library"} replace />} />
          <Route path="/statistics" element={statsOnly(<Statistics />)} />
          <Route path="/statistics/listening" element={statsOnly(<ListeningStatsPage />)} />
          <Route path="/statistics/library" element={statsOnly(<LibraryStatsPage />)} />
          <Route path="/tools" element={adminOnly(<Tools />)} />
          <Route path="/tools/duplicates" element={adminOnly(<Unduper />)} />
          <Route path="/library" element={<LibraryLayout />}>
            <Route index element={<LibraryHome />} />
            <Route path="home" element={<LibraryHome />} />
            <Route path="home/feed/:section" element={<LibraryHomeFeed />} />
            <Route path="artists" element={<LibraryArtists />} />
            <Route path="albums" element={<LibraryAlbums />} />
            <Route path="genres" element={<LibraryGenres />} />
            <Route path="labels" element={<LibraryLabels />} />
            <Route path="liked" element={<LikedPage />} />
            <Route path="recommendations" element={<RecommendationsPage />} />
            <Route path="artist/:artistId" element={<ArtistPage />} />
            <Route path="label/:label" element={<LabelPage />} />
            <Route path="genre/:genre" element={<GenrePage />} />
            <Route path="album/:albumId" element={<AlbumPage />} />
            <Route path="playlists" element={<Playlists />} />
            <Route path="playlists/:playlistId" element={<PlaylistDetail />} />
            <Route path="browser" element={<Navigate to="/library" replace />} />
          </Route>
          <Route path="/playlists" element={<Navigate to="/library/playlists" replace />} />
          <Route path="/playlists/:playlistId" element={<PlaylistLegacyRedirect />} />
          <Route path="/tag-fixer" element={adminOnly(<TagFixer />)} />
          <Route path="/broken-albums" element={adminOnly(<BrokenAlbumsList />)} />
          <Route path="/settings" element={<Settings />} />
          <Route path="/admin/users" element={adminOnly(<AdminUsersPage />)} />
          <Route path="*" element={<NotFound />} />
        </Route>
        <Route path="/auth/login" element={<Navigate to="/library" replace />} />
        <Route path="/auth/bootstrap" element={<Navigate to="/library" replace />} />
      </Routes>
      {auth.canUseAI ? <AssistantDock bottomOffsetPx={session ? 128 : 16} /> : null}
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
          <AuthProvider>
            <PlaybackProvider>
              <AppRoutesWithPlayer />
            </PlaybackProvider>
          </AuthProvider>
        </BrowserRouter>
      </TooltipProvider>
    </QueryClientProvider>
  </ThemeProvider>
);

export default App;
